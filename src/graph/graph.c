/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>

#include "graph.h"
#include "../util/arr.h"
#include "../util/qsort.h"
#include "../GraphBLASExt/GxB_Delete.h"
#include "../util/rmalloc.h"

static GrB_BinaryOp _graph_edge_accum = NULL;

GrB_Matrix _Graph_GetRelationMap(const Graph *g, int relation_idx);

/* ========================= Forward declarations  ========================= */
void _MatrixResizeToCapacity(const Graph *g, GrB_Matrix m);


/* ========================= GraphBLAS functions ========================= */
void _edge_accum(void *_z, const void *_x, const void *_y) {
    EdgeID *z = (EdgeID*)_z;
    const EdgeID *x = (const EdgeID*)_x;
    const EdgeID *y = (const EdgeID*)_y;

    EdgeID *ids;
    /* Single edge ID,
     * switching from single edge ID to multiple IDs. */
    if(SINGLE_EDGE(*x)) {
        ids = array_new(EdgeID, 2);
        ids = array_append(ids, SINGLE_EDGE_ID(*x));
        ids = array_append(ids, SINGLE_EDGE_ID(*y));
        // TODO: Make sure MSB of ids isn't on.
        *z = (EdgeID)ids;
    } else {
        // Multiple edges, adding another edge.
        ids = (EdgeID*)(*x);
        ids = array_append(ids, SINGLE_EDGE_ID(*y));
        *z = (EdgeID)ids;
    }
}

bool _select_op_free_edge(GrB_Index i, GrB_Index j, GrB_Index nrows, GrB_Index ncols, const void *x, const void *k) {
    const Graph *g = (const Graph*)k;
    const EdgeID *id = (const EdgeID*)x;
    if((SINGLE_EDGE(*id))) {
        DataBlock_DeleteItem(g->edges, SINGLE_EDGE_ID(*id));
    } else {
        EdgeID *ids = (EdgeID*)(*id);
        uint id_count = array_len(ids);
        for(uint i = 0; i < id_count; i++) {
            DataBlock_DeleteItem(g->edges, ids[i]);
        }
        array_free(ids);
    }

    return false;
}

/* ========================= Synchronization functions ========================= */

/* Acquire mutex when a reader thread may modify shared data. */
static inline void _Graph_EnterCriticalSection(Graph *g) {
    pthread_mutex_lock(&g->_mutex);
}

/* Release mutex. */
static inline void _Graph_LeaveCriticalSection(Graph *g) {
    pthread_mutex_unlock(&g->_mutex);
}

/* Acquire a lock that does not restrict access from additional reader threads */
void Graph_AcquireReadLock(Graph *g) {
    pthread_rwlock_rdlock(&g->_rwlock);
}

/* Acquire a lock for exclusive access to this graph's data */
void Graph_AcquireWriteLock(Graph *g) {
    pthread_rwlock_wrlock(&g->_rwlock);
    g->_writelocked = true;
}

/* Release the held lock */
void Graph_ReleaseLock(Graph *g) {
    g->_writelocked = false;
    pthread_rwlock_unlock(&g->_rwlock);
}

/* Writer request access to graph. */
void Graph_WriterEnter(Graph *g) {
    pthread_mutex_lock(&g->_writers_mutex);
}

/* Writer release access to graph. */
void Graph_WriterLeave(Graph *g) {
    pthread_mutex_unlock(&g->_writers_mutex);
}

/* Force execution of all pending operations on a matrix. */
static inline void _Graph_ApplyPending(GrB_Matrix m) {
    GrB_Index nvals;
    GrB_Matrix_nvals(&nvals, m);
}

/* ========================= Graph utility functions ========================= */

// Get the transposed adjacency matrix.
static GrB_Matrix _Graph_Get_Transposed_AdjacencyMatrix(const Graph *g) {
    assert(g);
    GrB_Matrix m = g->_t_adjacency_matrix;
    g->SynchronizeMatrix(g, m);
    return m;
}

// Return number of nodes graph can contain.
size_t _Graph_NodeCap(const Graph *g) {
    return g->nodes->itemCap;
}

// Return number of nodes graph can contain.
size_t _Graph_EdgeCap(const Graph *g) {
    return g->edges->itemCap;
}

// Retrieve a relation mapping matrix coresponding to relation_idx
// Make sure matrix is synchronized.
GrB_Matrix _Graph_GetRelationMap(const Graph *g, int relation_idx) {
    assert(g && relation_idx >= 0 && relation_idx < array_len(g->_relations_map));
    GrB_Matrix m = g->_relations_map[relation_idx];
    g->SynchronizeMatrix(g, m);
    return m;
}

// Create a new mapping matrix M,
// M[I,J] holds the edge ID connecting node J to I (CSC format)
// assuming _relations_map[K] holds mapping for relation K.
void _Graph_AddRelationMap(Graph *g) {
    GrB_Matrix mapper;
    GrB_Info res = GrB_Matrix_new(&mapper, GrB_UINT64, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
    assert(res == GrB_SUCCESS);
    g->_relations_map = array_append(g->_relations_map, mapper);
}

// Locates edges connecting src to destination.
void _Graph_GetEdgesConnectingNodes(const Graph *g, NodeID src, NodeID dest, int r, Edge **edges) {
    assert(g && src < Graph_RequiredMatrixDim(g) && dest < Graph_RequiredMatrixDim(g) && r < Graph_RelationTypeCount(g));

    Edge e;
    EdgeID edgeId;
    e.relationID = r;
    e.srcNodeID = src;
    e.destNodeID = dest;

    // relation map, maps (src, dest, r) to edge IDs.
    GrB_Matrix relationMap = _Graph_GetRelationMap(g, r);
    GrB_Info res = GrB_Matrix_extractElement_UINT64(&edgeId, relationMap, src, dest);

    // No entry at [dest, src], src is not connected to dest with relation R.
    if(res == GrB_NO_VALUE) return;

    if(SINGLE_EDGE(edgeId)) {
        // Discard most significate bit.
        edgeId = SINGLE_EDGE_ID(edgeId);
        e.entity = DataBlock_GetItem(g->edges, edgeId);
        assert(e.entity);
        *edges = array_append(*edges, e);
    } else {
        /* Multiple edges connecting src to dest,
         * entry is a pointer to an array of edge IDs. */
        EdgeID *edgeIds = (EdgeID*)edgeId;
        int edgeCount = array_len(edgeIds);

        for(int i = 0; i < edgeCount; i++) {
            edgeId = edgeIds[i];
            e.entity = DataBlock_GetItem(g->edges, edgeId);
            assert(e.entity);
            *edges = array_append(*edges, e);
        }
    }
}

static inline Entity *_Graph_GetEntity(const DataBlock *entities, EntityID id) {
    return DataBlock_GetItem(entities, id);
}

/* ============= Matrix synchronization and resizing functions =============== */

/* Resize given matrix, such that its number of row and columns
 * matches the number of nodes in the graph. Also, synchronize
 * matrix to execute any pending operations. */
void _MatrixSynchronize(const Graph *g, GrB_Matrix m) {
    GrB_Index n_rows;
    GrB_Matrix_nrows(&n_rows, m);

    // If the graph belongs to one thread, we don't need to flush pending operations
    // or lock the mutex.
    if (g->_writelocked) {
        if (n_rows != Graph_RequiredMatrixDim(g)) {
            assert(GxB_Matrix_resize(m, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g)) == GrB_SUCCESS);
        }
        return;
    }

    // If the matrix has pending operations or requires
    // a resize, enter critical section.
    bool pending = false;
    GxB_Matrix_Pending(m, &pending);
    if(pending || (n_rows != Graph_RequiredMatrixDim(g))) {
        _Graph_EnterCriticalSection((Graph *)g);
        // Double-check if resize is necessary.
        GrB_Matrix_nrows(&n_rows, m);
        if(n_rows != Graph_RequiredMatrixDim(g))
          assert(GxB_Matrix_resize(m, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g)) == GrB_SUCCESS);

        // Flush changes to matrices if necessary.
        GxB_Matrix_Pending(m, &pending);
        if (pending) _Graph_ApplyPending(m);

        _Graph_LeaveCriticalSection((Graph *)g);
    }
}

/* Resize matrix to node capacity. */
void _MatrixResizeToCapacity(const Graph *g, GrB_Matrix m) {
    GrB_Index ncols;
    GrB_Matrix_ncols(&ncols, m);

    if (ncols != _Graph_NodeCap(g)) {
      assert(GxB_Matrix_resize(m, _Graph_NodeCap(g), _Graph_NodeCap(g)) == GrB_SUCCESS);
    }
}

/* Do not update matrices. */
void _MatrixNOP(const Graph *g, GrB_Matrix m) {
    return;
}

/* Define the current behavior for matrix creations and retrievals on this graph. */
void Graph_SetMatrixPolicy(Graph *g, MATRIX_POLICY policy) {
    switch (policy) {
        case SYNC_AND_MINIMIZE_SPACE:
            // Default behavior; forces execution of pending GraphBLAS operations
            // when appropriate and sizes matrices to the current node count.
            g->SynchronizeMatrix = _MatrixSynchronize;
            break;
        case RESIZE_TO_CAPACITY:
            // Bulk insertion behavior; does not force pending operations
            // and resizes matrices to the graph's current node capacity.
            g->SynchronizeMatrix = _MatrixResizeToCapacity;
            break;
        case DISABLED:
            // Used when deleting or freeing a graph; forces no matrix updates or resizes.
            g->SynchronizeMatrix = _MatrixNOP;
            break;
        default:
            assert(false);
    }
}

/* Synchronize and resize all matrices in graph. */
void Graph_ApplyAllPending(Graph *g) {
    GrB_Matrix M;

    for(int i = 0; i < array_len(g->labels); i ++) {
      M = g->labels[i];
      g->SynchronizeMatrix(g, M);
    }

    for(int i = 0; i < array_len(g->relations); i ++) {
      M = g->relations[i];
      g->SynchronizeMatrix(g, M);
    }

    for(int i = 0; i < array_len(g->_relations_map); i ++) {
      M = g->_relations_map[i];
      g->SynchronizeMatrix(g, M);
    }
}

/* ================================ Graph API ================================ */
Graph *Graph_New(size_t node_cap, size_t edge_cap) {
    node_cap = MAX(node_cap, GRAPH_DEFAULT_NODE_CAP);
    edge_cap = MAX(node_cap, GRAPH_DEFAULT_EDGE_CAP);

    Graph *g = rm_malloc(sizeof(Graph));
    g->nodes = DataBlock_New(node_cap, sizeof(Entity), (fpDestructor)FreeEntity);
    g->edges = DataBlock_New(edge_cap, sizeof(Entity), (fpDestructor)FreeEntity);
    g->labels = array_new(GrB_Matrix, GRAPH_DEFAULT_LABEL_CAP);
    g->relations = array_new(GrB_Matrix, GRAPH_DEFAULT_RELATION_TYPE_CAP);
    g->_relations_map = array_new(GrB_Matrix, GRAPH_DEFAULT_RELATION_TYPE_CAP);
    GrB_Matrix_new(&g->adjacency_matrix, GrB_BOOL, node_cap, node_cap);
    GrB_Matrix_new(&g->_t_adjacency_matrix, GrB_BOOL, node_cap, node_cap);
    GrB_Matrix_new(&g->_zero_matrix, GrB_BOOL, node_cap, node_cap);

    // Initialize a read-write lock scoped to the individual graph
    assert(pthread_rwlock_init(&g->_rwlock, NULL) == 0);
    g->_writelocked = false;

    // Force GraphBLAS updates and resize matrices to node count by default
    Graph_SetMatrixPolicy(g, SYNC_AND_MINIMIZE_SPACE);

    /* TODO: We might want a mutex per matrix,
     * such that when a thread is resizing matrix A
     * another thread could be resizing matrix B. */
    assert(pthread_mutex_init(&g->_mutex, NULL) == 0);
    assert(pthread_mutex_init(&g->_writers_mutex, NULL) == 0);

    // Create edge accumulator binary function
    if(!_graph_edge_accum) {
        GrB_Info info;
        info = GrB_BinaryOp_new(&_graph_edge_accum, _edge_accum, GrB_UINT64, GrB_UINT64, GrB_UINT64);
        assert(info == GrB_SUCCESS);
    }

    return g;
}

// All graph matrices are required to be squared NXN
// where N is Graph_RequiredMatrixDim.
size_t Graph_RequiredMatrixDim(const Graph *g) {
    // Matrix dimensions should be at least:
    // Number of nodes + number of deleted nodes.
    return g->nodes->itemCount + array_len(g->nodes->deletedIdx);
}

size_t Graph_NodeCount(const Graph *g) {
    assert(g);
    return g->nodes->itemCount;
}

size_t Graph_LabeledNodeCount(const Graph *g, int label) {
    GrB_Index nvals = 0;
    GrB_Matrix m = Graph_GetLabelMatrix(g, label);
    if(m) GrB_Matrix_nvals(&nvals, m);
    return nvals;
}

size_t Graph_EdgeCount(const Graph *g) {
    assert(g);
    return g->edges->itemCount;
}

int Graph_RelationTypeCount(const Graph *g) {
    return array_len(g->relations);
}

int Graph_LabelTypeCount(const Graph *g) {
    return array_len(g->labels);
}

void Graph_AllocateNodes(Graph* g, size_t n) {
    assert(g);
    DataBlock_Accommodate(g->nodes, n);
}

void Graph_AllocateEdges(Graph *g, size_t n) {
    assert(g);
    DataBlock_Accommodate(g->edges, n);
}

int Graph_GetNode(const Graph *g, NodeID id, Node *n) {
    assert(g);
    n->entity = _Graph_GetEntity(g->nodes, id);
    return (n->entity!=NULL);
}

int Graph_GetEdge(const Graph *g, EdgeID id, Edge *e) {
    assert(g && id < _Graph_EdgeCap(g));
    e->entity = _Graph_GetEntity(g->edges, id);
    return (e->entity!=NULL);
}

int Graph_GetNodeLabel(const Graph *g, NodeID nodeID) {
    assert(g);
    int label = GRAPH_NO_LABEL;
    for(int i = 0; i < array_len(g->labels); i++) {
        bool x = false;
        GrB_Matrix M = Graph_GetLabelMatrix(g, i);
        GrB_Info res = GrB_Matrix_extractElement_BOOL(&x, M, nodeID, nodeID);
        if(res == GrB_SUCCESS && x == true) {
            label = i;
            break;
        }
    }

    return label;
}

int Graph_GetEdgeRelation(const Graph *g, Edge *e) {
    assert(g && e);
    NodeID srcNodeID = Edge_GetSrcNodeID(e);
    NodeID destNodeID = Edge_GetDestNodeID(e);    
    EdgeID id = ENTITY_GET_ID(e);

    // Search for relation mapping matrix M, where
    // M[dest,src] == edge ID.
    for(int i = 0; i < array_len(g->_relations_map); i++) {
        EdgeID edgeId = 0;
        GrB_Matrix M = _Graph_GetRelationMap(g, i);
        GrB_Info res = GrB_Matrix_extractElement_UINT64(&edgeId, M, srcNodeID, destNodeID);
        if(res != GrB_SUCCESS) continue;

        if(SINGLE_EDGE(edgeId)) {
			EdgeID curEdgeID = SINGLE_EDGE_ID(edgeId);
			if(curEdgeID == id) {
				Edge_SetRelationID(e, i);
				return i;
			}
        } else {
            /* Multiple edges exists between src and dest
             * see if given edge is one of them. */
            EdgeID *edges = (EdgeID*)edgeId;
            int edge_count = array_len(edges);
            for(int j = 0; j < edge_count; j++) {
                if(edges[j] == id) {
                    Edge_SetRelationID(e, i);
                    return i;
                }
            }
        }
    }

    // We must be able to find edge relation.
    assert(false);
    return GRAPH_NO_RELATION;
}

void Graph_GetEdgesConnectingNodes(const Graph *g, NodeID srcID, NodeID destID, int r, Edge **edges) {
    assert(g && r < Graph_RelationTypeCount(g) && edges);

    Node srcNode;
    Node destNode;
    assert(Graph_GetNode(g, srcID, &srcNode));
    assert(Graph_GetNode(g, destID, &destNode));

    if(r != GRAPH_NO_RELATION) {
      _Graph_GetEdgesConnectingNodes(g, srcID, destID, r, edges);
    } else {
        // Relation type missing, scan through each edge type.
        int relationCount = Graph_RelationTypeCount(g);
        for(int i = 0; i < relationCount; i++) {
            _Graph_GetEdgesConnectingNodes(g, srcID, destID, i, edges);
        }
    }
}

void Graph_CreateNode(Graph* g, int label, Node *n) {
    assert(g);

    NodeID id;
    Entity *en = DataBlock_AllocateItem(g->nodes, &id);
    en->id = id;
    en->prop_count = 0;
    en->properties = NULL;
    n->entity = en;

    if(label != GRAPH_NO_LABEL) {
        // Try to set matrix at position [id, id]
        // incase of a failure, scale matrix.
        GrB_Matrix m = g->labels[label];
        GrB_Info res = GrB_Matrix_setElement_BOOL(m, true, id, id);
        if(res != GrB_SUCCESS) {
            _MatrixResizeToCapacity(g, m);
            assert(GrB_Matrix_setElement_BOOL(m, true, id, id) == GrB_SUCCESS);
        }
    }
}

int Graph_ConnectNodes(Graph *g, NodeID src, NodeID dest, int r, Edge *e) {
    GrB_Info info;
    Node srcNode;
    Node destNode;

    assert(Graph_GetNode(g, src, &srcNode));
    assert(Graph_GetNode(g, dest, &destNode));
    assert(g && r < Graph_RelationTypeCount(g));

    EdgeID id;
    Entity *en = DataBlock_AllocateItem(g->edges, &id);
    en->id = id;
    en->prop_count = 0;
    en->properties = NULL;
    e->entity = en;
    e->relationID = r;
    e->srcNodeID = src;
    e->destNodeID = dest;

    GrB_Matrix adj = Graph_GetAdjacencyMatrix(g);
    GrB_Matrix relationMat = Graph_GetRelationMatrix(g, r);
    GrB_Matrix relationMapMat = _Graph_GetRelationMap(g, r);
    GrB_Matrix tadj = _Graph_Get_Transposed_AdjacencyMatrix(g);

    // Rows represent source nodes, columns represent destination nodes.
    GrB_Matrix_setElement_BOOL(adj, true, src, dest);
    GrB_Matrix_setElement_BOOL(tadj, true, dest, src);
    GrB_Matrix_setElement_BOOL(relationMat, true, src, dest);
    GrB_Index I = src;
    GrB_Index J = dest;
    id = SET_MSB(id);
    info = GxB_Matrix_subassign_UINT64   // C(I,J)<Mask> = accum (C(I,J),x)
    (
        relationMapMat,     // input/output matrix for results
        GrB_NULL,           // optional mask for C(I,J), unused if NULL
        _graph_edge_accum,  // optional accum for Z=accum(C(I,J),x)
        id,                 // scalar to assign to C(I,J)
        &I,                 // row indices
        1,                  // number of row indices
        &J,                 // column indices
        1,                  // number of column indices
        GrB_NULL            // descriptor for C(I,J) and Mask
    );
    assert(info == GrB_SUCCESS);

    return 1;
}

/* Retrieves all either incoming or outgoing edges 
 * to/from given node N, depending on given direction. */
void Graph_GetNodeEdges(const Graph *g, const Node *n, GRAPH_EDGE_DIR dir, int edgeType, Edge **edges) {
    assert(g && n && edges);
    GrB_Matrix M;
    NodeID srcNodeID;
    NodeID destNodeID;
    GxB_MatrixTupleIter *tupleIter;
    if(edgeType == GRAPH_NO_RELATION) M = Graph_GetAdjacencyMatrix(g);
    else M = Graph_GetRelationMatrix(g, edgeType);

    // Outgoing.
    if(dir == GRAPH_EDGE_DIR_OUTGOING || dir == GRAPH_EDGE_DIR_BOTH) {
        GxB_MatrixTupleIter_new(&tupleIter, M);
        srcNodeID = ENTITY_GET_ID(n);
        GxB_MatrixTupleIter_iterate_row(tupleIter, srcNodeID);
        while (true)
        {
            bool depleted = false;
            GxB_MatrixTupleIter_next(tupleIter, NULL, &destNodeID, &depleted);
            // GxB_MatrixTupleIter_next(tupleIter, &destNodeID, NULL, &depleted);
            if(depleted) break;
            Graph_GetEdgesConnectingNodes(g, srcNodeID, destNodeID, edgeType, edges);
        }
        GxB_MatrixTupleIter_free(tupleIter);
    }

    // Incoming.
    if(dir == GRAPH_EDGE_DIR_INCOMING || dir == GRAPH_EDGE_DIR_BOTH) {
        destNodeID = ENTITY_GET_ID(n);
        GrB_Vector incoming = GrB_NULL;
        GrB_Descriptor desc = GrB_NULL;

        if(edgeType == GRAPH_NO_RELATION) {
            // Relation wasn't specified, use transposed adjacency matrix.
            M = _Graph_Get_Transposed_AdjacencyMatrix(g);
            GxB_MatrixTupleIter_new(&tupleIter, M);
            GxB_MatrixTupleIter_iterate_row(tupleIter, destNodeID);
        } else {
            // TODO: Callers wishing to get Incoming edges to a number of nodes
            // should pass a transposed matrix, as the operations below are costly
            // and we'll perform them forevery node.
            size_t nRows = Graph_RequiredMatrixDim(g);
            GrB_Vector_new(&incoming, GrB_BOOL, nRows);
            GrB_Descriptor_new(&desc);
            GrB_Descriptor_set(desc, GrB_INP0, GrB_TRAN);
            GrB_Col_extract(incoming, NULL, NULL, M, GrB_ALL, nRows, destNodeID, desc);
            GxB_MatrixTupleIter_new(&tupleIter, (GrB_Matrix)incoming);
        }

        while(true) {
            bool depleted = false;
            GxB_MatrixTupleIter_next(tupleIter, NULL, &srcNodeID, &depleted);
            if(depleted) break;
            Graph_GetEdgesConnectingNodes(g, srcNodeID, destNodeID, edgeType, edges);
        }

        // Clean up
        GxB_MatrixTupleIter_free(tupleIter);
        if(desc != GrB_NULL) GrB_Descriptor_free(&desc);
        if(incoming != GrB_NULL) GrB_Vector_free(&incoming);
    }
}

/* Removes an edge from Graph and updates graph relevent matrices. */
int Graph_DeleteEdge(Graph *g, Edge *e) {
    bool x;
    GrB_Matrix R;
    GrB_Matrix M;
    GrB_Info info;
    EdgeID edge_id;
    int r = Edge_GetRelationID(e);
    NodeID src_id = Edge_GetSrcNodeID(e);
    NodeID dest_id = Edge_GetDestNodeID(e);

    R = _Graph_GetRelationMap(g, r);
    M = Graph_GetRelationMatrix(g, r);

    // Test to see if edge exists.
    info = GrB_Matrix_extractElement_BOOL(&x, M, src_id, dest_id);
    if(info != GrB_SUCCESS) return 0;

    GrB_Matrix_extractElement_UINT64(&edge_id, R, src_id, dest_id);

    if(SINGLE_EDGE(edge_id)) {
        /* Single edge of type R connecting src to dest.
         * delete entry from both M and R. */
        assert(GxB_Matrix_Delete(M, src_id, dest_id) == GrB_SUCCESS);        
        assert(GxB_Matrix_Delete(R, src_id, dest_id )== GrB_SUCCESS);
    
        // See if source is connected to destination with additional edges.
        bool connected = false;
        int relationCount = Graph_RelationTypeCount(g);
        for(int i = 0; i < relationCount; i++) {
            if(i == r) continue;
            M = Graph_GetRelationMatrix(g, i);
            info = GrB_Matrix_extractElement_BOOL(&connected, M, src_id, dest_id);
            if(info == GrB_SUCCESS) break;
        }

        /* There are no additional edges connecting source to destination
         * Remove edge from THE adjacency matrix. */
        if(!connected) {
            M = Graph_GetAdjacencyMatrix(g);
            assert(GxB_Matrix_Delete(M, src_id, dest_id) == GrB_SUCCESS);

            M = _Graph_Get_Transposed_AdjacencyMatrix(g);
            assert(GxB_Matrix_Delete(M, dest_id, src_id) == GrB_SUCCESS);
        }
    } else {
        /* Multiple edges connecting src to dest
         * locate specific edge and remove it
         * revert back from array representation to edge ID
         * incase we're left with a single edge connecting src to dest. */

        int i = 0;
        EdgeID id = ENTITY_GET_ID(e);
        EdgeID *edges = (EdgeID*)edge_id;
        int edge_count = array_len(edges);

        // Locate edge within edge array.
        for(; i < edge_count; i++) {
            if(edges[i] == id) break;
        }
        assert(i < edge_count);

        /* Remove edge from edge array
         * migrate last edge ID and reduce array size.
         * TODO: reallocate array of size / capacity ratio is high. */
        edges[i] = edges[edge_count-1];
        array_pop(edges);

        /* Incase we're left with a single edge connecting src to dest
         * revert back from array to scalar. */
        if(array_len(edges) == 1) {
            edge_id = edges[0];
            array_free(edges);
            GrB_Matrix_setElement_UINT64(R, SET_MSB(edge_id), src_id, dest_id);
        }
    }

    // Free and remove edges from datablock.
    DataBlock_DeleteItem(g->edges, ENTITY_GET_ID(e));
    return 1;
}

void Graph_DeleteNode(Graph *g, Node *n) {
    /* Assumption, node is completely detected, 
     * there are no incoming nor outgoing edges
     * leading to / from node. */
    assert(g && n);
    
    // Clear label matrix at position node ID.
    uint32_t label_count = array_len(g->labels);
    for(int i = 0; i < label_count; i++) {
        GrB_Matrix M = Graph_GetLabelMatrix(g, i);
        GxB_Matrix_Delete(M, ENTITY_GET_ID(n), ENTITY_GET_ID(n));
    }

    DataBlock_DeleteItem(g->nodes, ENTITY_GET_ID(n));
}

void _BulkDeleteNodes(Graph *g, Node *nodes, uint node_count,
                      uint *node_deleted, uint *edge_deleted) {
    assert(g && g->_writelocked && nodes && node_count > 0);

    /* Create a matrix M where M[j,i] = 1 where:
     * Node i in is connected to node j. */
    
    GrB_Matrix A;                       // A = R(M) masked relation matrix.
    GrB_Index nvals;                    // Number of elements in mask.
    GrB_Matrix Mask;                    // Mask noteing all implicitly deleted edges.
    GrB_Matrix Nodes;                   // Mask noteing each node marked for deletion.
    GrB_Matrix adj;                     // Adjacency matrix.
    GrB_Matrix tadj;                    // Transposed adjacency matrix.
    GrB_Descriptor desc;                // GraphBLAS descriptor.
    GxB_SelectOp selectop;              // GraphBLAS select operator, used to free edges.
    GxB_MatrixTupleIter *adj_iter;      // iterator over the adjacency matrix.
    GxB_MatrixTupleIter *tadj_iter;     // iterator over the transposed adjacency matrix.

    GrB_Descriptor_new(&desc);
    adj = Graph_GetAdjacencyMatrix(g);
    tadj = _Graph_Get_Transposed_AdjacencyMatrix(g);
    GxB_MatrixTupleIter_new(&adj_iter, adj);
    GxB_MatrixTupleIter_new(&tadj_iter, tadj);
    GxB_SelectOp_new(&selectop, _select_op_free_edge, GrB_UINT64);
    GrB_Matrix_new(&A, GrB_UINT64, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
    GrB_Matrix_new(&Mask, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));    
    GrB_Matrix_new(&Nodes, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));

    // Populate mask with implicit edges, take note of deleted nodes.
    for(uint i = 0; i < node_count; i++) {
        GrB_Index src;
        GrB_Index dest;
        Node *n = nodes + i;
        bool depleted = false;
        NodeID ID = ENTITY_GET_ID(n);

        // Outgoing edges.
        GxB_MatrixTupleIter_iterate_row(adj_iter, ID);
        while(true) {
            GxB_MatrixTupleIter_next(adj_iter, NULL,  &dest, &depleted);
            if(depleted) break;
            GrB_Matrix_setElement_BOOL(Mask, true, ID, dest);
        }

        depleted = false;

        // Incoming edges.
        GxB_MatrixTupleIter_iterate_row(tadj_iter, ID);
        while(true) {
            GxB_MatrixTupleIter_next(tadj_iter, NULL, &src, &depleted);
            if(depleted) break;
            GrB_Matrix_setElement_BOOL(Mask, true, src, ID);
        }

        GrB_Matrix_setElement_BOOL(Nodes, true, ID, ID);
    }
    
    GrB_Matrix_nvals(&nvals, Nodes);
    *node_deleted += nvals;

    GrB_Matrix_nvals(&nvals, Mask);
    *edge_deleted += nvals;

    // Clear updated output matrix before assignment.
    GrB_Descriptor_set(desc, GrB_OUTP, GrB_REPLACE);

    // Free and remove implicit edges from relation matrices.
    int relation_count = Graph_RelationTypeCount(g);
    for(int i = 0; i < relation_count; i++) {
        GrB_Matrix R = _Graph_GetRelationMap(g, i);
        
        // Reset mask descriptor.
        GrB_Descriptor_set(desc, GrB_MASK, GxB_DEFAULT);

        /* Isolate implicit edges.
         * A will contain all implicitly deleted edges from R. */
        GrB_Matrix_apply(A, Mask, NULL, GrB_IDENTITY_UINT64, R, desc);
        
        /* Free each multi edge array entry in A
         * Call _select_op_free_edge on each entry of A. */
        GxB_select(A, GrB_NULL, GrB_NULL, selectop, A, g, GrB_NULL);

        // Clear both relation matrix and its coresponding relation mapping matrix.
        GrB_Descriptor_set(desc, GrB_MASK, GrB_SCMP);

        // Remove every entry of R marked by Mask.
        GrB_Matrix_apply(R, Mask, NULL, GrB_IDENTITY_UINT64, R, desc);

        R = Graph_GetRelationMatrix(g, i);
        // Remove every entry of R marked by Mask.
        GrB_Matrix_apply(R, Mask, NULL, GrB_IDENTITY_UINT64, R, desc);
    }

    /* Descriptor:
     * GrB_MASK, GrB_SCMP
     * GrB_OUTP, GrB_REPLACE */
    GrB_Descriptor_set(desc, GrB_MASK, GrB_SCMP);

    // Update Adjacency and transposed adjacency matrices.
    GrB_Matrix_apply(adj, Mask, NULL, GrB_IDENTITY_UINT64, adj, desc);
    GrB_Matrix_apply(tadj, Mask, NULL, GrB_IDENTITY_UINT64, tadj, desc);

    /* Delete nodes
     * All nodes marked for deleteion are detected, no incoming / outgoing edges. */
    int node_type_count = Graph_LabelTypeCount(g);
    for(int i = 0; i < node_type_count; i++) {
        GrB_Matrix L = Graph_GetLabelMatrix(g, i);
        GrB_Matrix_apply(L, Nodes, NULL, GrB_IDENTITY_BOOL, L, desc);
    }

    for(uint i = 0; i < node_count; i++) {
        Node *n = nodes + i;
        DataBlock_DeleteItem(g->nodes, ENTITY_GET_ID(n));
    }

    // Clean up.
    GrB_free(&A);
    GrB_free(&desc);
    GrB_free(&Mask);
    GrB_free(&Nodes);
    GrB_free(&selectop);
    GxB_MatrixTupleIter_free(adj_iter);
    GxB_MatrixTupleIter_free(tadj_iter);
}

void _BulkDeleteEdges(Graph *g, Edge *edges, size_t edge_count) {
    assert(g && g->_writelocked && edges && edge_count > 0);

    // Describe a matrix entry deletion.
    typedef struct {
        GrB_Matrix M;   // Matrix being modified.
        GrB_Index row;  // Row index
        GrB_Index col;  // Column index.
    } PendingDeletion;

    GrB_Matrix R;   // Relation matrix.
    GrB_Matrix M;   // Relation mapping matrix.
    GrB_Info info;
    EdgeID edge_id;
    
    PendingDeletion deletion;
    PendingDeletion *deletions = array_new(PendingDeletion, edge_count*2);

    for(int i = 0; i < edge_count; i++) {
        Edge *e = edges + i;
        int r = Edge_GetRelationID(e);
        NodeID src_id = Edge_GetSrcNodeID(e);
        NodeID dest_id = Edge_GetDestNodeID(e);

        M = _Graph_GetRelationMap(g, r);
        R = Graph_GetRelationMatrix(g, r);

        GrB_Matrix_extractElement_UINT64(&edge_id, M, src_id, dest_id);

        if(SINGLE_EDGE(edge_id)) {
            /* Single edge of type R connecting src to dest.
             * delete entry from both M and R. */
            deletion.M = R;
            deletion.row = src_id;
            deletion.col = dest_id;
            deletions = array_append(deletions, deletion);

            deletion.M = M;
            deletions = array_append(deletions, deletion);
        } else {
            /* Multiple edges connecting src to dest
             * locate specific edge and remove it
             * revert back from array representation to edge ID
             * incase we're left with a single edge connecting src to dest. */

            int i = 0;
            EdgeID id = ENTITY_GET_ID(e);
            EdgeID *multi_edges = (EdgeID*)edge_id;
            int multi_edge_count = array_len(multi_edges);

            // Locate edge within edge array.
            for(; i < multi_edge_count; i++) {
                if(multi_edges[i] == id) break;
            }
            assert(i < multi_edge_count);

            /* Remove edge from edge array
             * migrate last edge ID and reduce array size.
             * TODO: reallocate array of size / capacity ratio is high. */
            multi_edges[i] = multi_edges[multi_edge_count-1];
            array_pop(multi_edges);

            /* Incase we're left with a single edge connecting src to dest
             * revert back from array to scalar. */
            if(array_len(multi_edges) == 1) {
                edge_id = multi_edges[0];
                array_free(multi_edges);
                GrB_Matrix_setElement_UINT64(M, SET_MSB(edge_id), src_id, dest_id);
            }
        }

        // Free and remove edges from datablock.
        DataBlock_DeleteItem(g->edges, ENTITY_GET_ID(e));
    }

    // Delete entries.
    for(int i = 0; i < array_len(deletions); i++) {
        deletion = deletions[i];
        assert(GxB_Matrix_Delete(deletion.M, deletion.row, deletion.col) == GrB_SUCCESS);
    }

    int relationCount = Graph_RelationTypeCount(g);
    uint deletion_count = array_len(deletions);
    for(uint i = 0; i < deletion_count; i++) {
        deletion = deletions[i];
        GrB_Index src = deletion.row;
        GrB_Index dest = deletion.col;

        // See if source is connected to destination with additional edges.
        bool connected = false;
        for(int i = 0; i < relationCount; i++) {
            R = Graph_GetRelationMatrix(g, i);
            info = GrB_Matrix_extractElement_BOOL(&connected, R, src, dest);
            if(info == GrB_SUCCESS) break;
        }

        /* There are no additional edges connecting source to destination
         * Remove edge from THE adjacency matrix. 
         * It is OK to remove entries from the adjacency matrix, as we're 
         * not trying to extract entries from it. */
        if(!connected) {
            M = Graph_GetAdjacencyMatrix(g);
            assert(GxB_Matrix_Delete(M, src, dest) == GrB_SUCCESS);

            M = _Graph_Get_Transposed_AdjacencyMatrix(g);
            assert(GxB_Matrix_Delete(M, dest, src) == GrB_SUCCESS);
        }
    }

    // Clean up.
    array_free(deletions);
}

/* Removes both nodes and edges from graph. */
void Graph_BulkDelete(Graph *g, Node *nodes, uint node_count, Edge *edges, uint edge_count, uint *node_deleted, uint *edge_deleted) {
    assert(g);

    *edge_deleted = 0;
    *node_deleted = 0;

    if(node_count) _BulkDeleteNodes(g, nodes, node_count, node_deleted, edge_deleted);

    if(edge_count) {
        // Filter out explicit edges which were removed by _BulkDeleteNodes.
        if(node_count) {
            for(int i = 0; i < edge_count; i++) {
                Edge *e = edges+i;
                NodeID src = Edge_GetSrcNodeID(e);
                NodeID dest = Edge_GetDestNodeID(e);
                
                if(!DataBlock_GetItem(g->nodes, src) || !DataBlock_GetItem(g->nodes, dest)) {
                    /* Edge already removed due to node removal.
                    * Replace current edge with last edge. */
                    edges[i] = edges[edge_count-1];

                    // Update indices.
                    i--;
                    edge_count--;
                }
            }
        }

        /* it might be that edge_count dropped to 0
         * due to implicit edge deletion. */
        if(edge_count == 0) return;

        // Removing duplicates.
        #define is_edge_lt(a, b) (ENTITY_GET_ID((a)) < ENTITY_GET_ID((b)))
        QSORT(Edge, edges, edge_count, is_edge_lt);

        size_t uniqueIdx = 0;
        for(int i = 0; i < edge_count; i++) {
            // As long as current is the same as follows.
            while(i < edge_count-1 && ENTITY_GET_ID(edges+i) == ENTITY_GET_ID(edges+i+1)) i++;

            if(uniqueIdx < i) edges[uniqueIdx] = edges[i];
            uniqueIdx++;
        }

        edge_count = uniqueIdx;
        _BulkDeleteEdges(g, edges, edge_count);
    }

    *edge_deleted += edge_count;
}

DataBlockIterator *Graph_ScanNodes(const Graph *g) {
    assert(g);
    return DataBlock_Scan(g->nodes);
}

DataBlockIterator *Graph_ScanEdges(const Graph *g) {
    assert(g);
    return DataBlock_Scan(g->edges);
}

int Graph_AddLabel(Graph *g) {
    assert(g);

    GrB_Matrix m;
    GrB_Matrix_new(&m, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
    array_append(g->labels, m);
    return array_len(g->labels)-1;
}

int Graph_AddRelationType(Graph *g) {
    assert(g);

    GrB_Matrix m;
    GrB_Matrix_new(&m, GrB_BOOL, Graph_RequiredMatrixDim(g), Graph_RequiredMatrixDim(g));
    g->relations = array_append(g->relations, m);

    _Graph_AddRelationMap(g);

    // Edge mapping for relation K is at _relations_map[K].
    assert(array_len(g->_relations_map) == Graph_RelationTypeCount(g));
    int relationID = Graph_RelationTypeCount(g)-1;
    return relationID;
}

GrB_Matrix Graph_GetAdjacencyMatrix(const Graph *g) {
    assert(g);
    GrB_Matrix m = g->adjacency_matrix;
    g->SynchronizeMatrix(g, m);
    return m;
}

GrB_Matrix Graph_GetLabelMatrix(const Graph *g, int label_idx) {
    assert(g && label_idx < array_len(g->labels));
    GrB_Matrix m = g->labels[label_idx];
    g->SynchronizeMatrix(g, m);
    return m;
}

GrB_Matrix Graph_GetRelationMatrix(const Graph *g, int relation_idx) {
    assert(g && (relation_idx == GRAPH_NO_RELATION || relation_idx < Graph_RelationTypeCount(g)));
    GrB_Matrix m;

    if(relation_idx == GRAPH_NO_RELATION) {
        m = Graph_GetAdjacencyMatrix(g);
    } else {
        m = g->relations[relation_idx];
        g->SynchronizeMatrix(g, m);
    }
    return m;
}

GrB_Matrix Graph_GetZeroMatrix(const Graph *g) {
    GrB_Index nvals;
    GrB_Matrix z = g->_zero_matrix;
    g->SynchronizeMatrix(g, z);

    // Make sure zero matrix is indeed empty.
    GrB_Matrix_nvals(&nvals, z);
    assert(nvals == 0);
    return z;
}

void Graph_Free(Graph *g) {
    assert(g);
    // Free matrices.
    Entity *en;
    DataBlockIterator *it;
    GrB_Matrix z = Graph_GetZeroMatrix(g);
    GrB_Matrix m = Graph_GetAdjacencyMatrix(g);
    GrB_Matrix tm = _Graph_Get_Transposed_AdjacencyMatrix(g);
    GrB_Matrix_free(&m);
    GrB_Matrix_free(&z);
    GrB_Matrix_free(&tm);

    uint32_t relationCount = Graph_RelationTypeCount(g);
    for(int i = 0; i < relationCount; i++) {
        m = g->relations[i];
        GrB_Matrix_free(&m);
        m = g->_relations_map[i];
        GrB_Matrix_free(&m);
    }
    array_free(g->relations);
    array_free(g->_relations_map);

    uint32_t labelCount = array_len(g->labels);
    for(int i = 0; i < labelCount; i++) {
        m = g->labels[i];
        GrB_Matrix_free(&m);
    }
    array_free(g->labels);

    it = Graph_ScanNodes(g);
    while ((en = (Entity*)DataBlockIterator_Next(it)) != NULL)
        FreeEntity(en);

    DataBlockIterator_Free(it);

    it = Graph_ScanEdges(g);
    while ((en = DataBlockIterator_Next(it)) != NULL)
        FreeEntity(en);

    DataBlockIterator_Free(it);

    // Free blocks.
    DataBlock_Free(g->nodes);
    DataBlock_Free(g->edges);

    // Destroy graph-scoped locks.
    assert(pthread_mutex_destroy(&g->_mutex) == 0);
    assert(pthread_mutex_destroy(&g->_writers_mutex) == 0);

    if(g->_writelocked) Graph_ReleaseLock(g);
    assert(pthread_rwlock_destroy(&g->_rwlock) == 0);

    rm_free(g);
}
