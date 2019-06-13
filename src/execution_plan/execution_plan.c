/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>

#include "execution_plan.h"
#include "./ops/ops.h"
#include "../util/rmalloc.h"
#include "../util/arr.h"
#include "../util/vector.h"
#include "../graph/entities/edge.h"
#include "./optimizations/optimizer.h"
#include "./optimizations/optimizations.h"
#include "../arithmetic/algebraic_expression.h"
#include "../ast/ast_build_op_contexts.h"
#include "../ast/ast_build_filter_tree.h"

static ResultSet* _prepare_resultset(RedisModuleCtx *ctx, AST *ast, bool compact) {
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    bool distinct = false;
    if (ret_clause) {
        distinct = cypher_ast_return_is_distinct(ret_clause);
    }
    ResultSet *set = NewResultSet(ctx, distinct, compact);
    return set;
}

AR_ExpNode** _ReturnExpandAll(RecordMap *record_map) {
    AST *ast = AST_GetFromTLS();

    TrieMap *aliases = AST_CollectAliases(ast);
    uint count = aliases->cardinality;

    AR_ExpNode **return_expressions = array_new(AR_ExpNode*, count);
    void *value;
    tm_len_t len;
    char *key;
    TrieMapIterator *it = TrieMap_Iterate(aliases, "", 0);
    while(TrieMapIterator_Next(it, &key, &len, &value)) {
        AR_ExpNode *exp = AR_EXP_NewVariableOperandNode(record_map, (const char *)key, NULL);
        exp->resolved_name = key;
        return_expressions = array_append(return_expressions, exp);
    }

    return return_expressions;
}

// Handle ORDER entities
AR_ExpNode** _BuildOrderExpressions(RecordMap *record_map, AR_ExpNode **projections, const cypher_astnode_t *order_clause) {
    bool ascending = true;

    uint projection_count = array_len(projections);
    uint count = cypher_ast_order_by_nitems(order_clause);
    AR_ExpNode **order_exps = array_new(AR_ExpNode*, count);

    for (uint i = 0; i < count; i++) {
        const cypher_astnode_t *item = cypher_ast_order_by_get_item(order_clause, i);
        const cypher_astnode_t *ast_exp = cypher_ast_sort_item_get_expression(item);
        /* TODO need to think about logic here - can introduce new data, reference
         * projections, reference otherwise-unprojected aliases. In the referencing-projection
         * case, we may not be allowed to use the pre-existing record index:
         * RETURN e.name as v ORDER BY v */
        AR_ExpNode *exp;
        if (cypher_astnode_type(ast_exp) == CYPHER_AST_IDENTIFIER) {
            // Order expression is a reference to an alias in the query
            const char *alias = cypher_ast_identifier_get_name(ast_exp);
            for (uint j = 0; j < projection_count; j ++) {
                AR_ExpNode *projection = projections[j];
                if (!strcmp(projection->resolved_name, alias)) {
                    exp = projection;
                }
            }
            // uint record_id = RecordMap_LookupAlias(record_map, alias);
            // if (record_id != IDENTIFIER_NOT_FOUND) {
                // // check projections?
            // }
            // // Clone the expression so that we can free safely
            // // assert(false);
            // exp = AR_EXP_Clone(AST_GetEntityFromAlias(ast, alias));
        } else {
            // Independent operator like:
            // ORDER BY COUNT(a)
            exp = AR_EXP_FromExpression(record_map, ast_exp);
        }
        // AR_ExpNode *exp = AR_EXP_FromExpression(record_map, ast_exp);

        order_exps = array_append(order_exps, exp);
        // TODO direction should be specifiable per order entity
        ascending = cypher_ast_sort_item_is_ascending(item);
    }

    // *direction = ascending ? DIR_ASC : DIR_DESC;

    return order_exps;
}

// Handle RETURN entities
AR_ExpNode** _BuildReturnExpressions(RecordMap *record_map, const cypher_astnode_t *ret_clause) {
    // Query is of type "RETURN *",
    // collect all defined identifiers and create return elements for them
    if (cypher_ast_return_has_include_existing(ret_clause)) return _ReturnExpandAll(record_map);

    uint count = cypher_ast_return_nprojections(ret_clause);
    AR_ExpNode **return_expressions = array_new(AR_ExpNode*, count);
    for (uint i = 0; i < count; i++) {
        const cypher_astnode_t *projection = cypher_ast_return_get_projection(ret_clause, i);
        // The AST expression can be an identifier, function call, or constant
        const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);

        // Construction an AR_ExpNode to represent this return entity.
        AR_ExpNode *exp = AR_EXP_FromExpression(record_map, ast_exp);


        // Find the resolved name of the entity - its alias, its identifier if referring to a full entity,
        // the entity.prop combination ("a.val"), or the function call ("MAX(a.val)")
        const char *identifier = NULL;
        const cypher_astnode_t *alias_node = cypher_ast_projection_get_alias(projection);
        if (alias_node) {
            // The projection either has an alias (AS), is a function call, or is a property specification (e.name).
            identifier = cypher_ast_identifier_get_name(alias_node);
            // TODO not quite correct, since re-uses the record ID but we actually want to refer to the
            // just-built AR_ExpNode
            // For the moment, use master logic? (No.)
            // AR_ExpNode *alias_exp = AR_EXP_Clone(exp);
            // AR_ExpNode *alias_exp = AR_EXP_NewVariableOperandNode(record_map, alias, NULL);
            // AR_ExpNode *alias_exp = AR_EXP_NewReferenceNode(i);
            // RecordMap_AssociateAliasWithID(record_map, alias, exp->operand.variadic.entity_alias_idx);
            // RecordMap_LookupAlias(segment->record_map, alias);
        } else {
            // This expression did not have an alias, so it must be an identifier
            const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);
            assert(cypher_astnode_type(ast_exp) == CYPHER_AST_IDENTIFIER);
            // Retrieve "a" from "RETURN a" or "RETURN a AS e" (theoretically; the latter case is already handled)
            identifier = cypher_ast_identifier_get_name(ast_exp);
        }

        exp->resolved_name = identifier;

        return_expressions = array_append(return_expressions, exp);
    }

    return return_expressions;
}

AR_ExpNode** _BuildWithExpressions(RecordMap *record_map, const cypher_astnode_t *with_clause) {
    uint count = cypher_ast_with_nprojections(with_clause);
    AR_ExpNode **with_expressions = array_new(AR_ExpNode*, count);
    for (uint i = 0; i < count; i++) {
        const cypher_astnode_t *projection = cypher_ast_with_get_projection(with_clause, i);
        const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);

        // Construction an AR_ExpNode to represent this entity.
        AR_ExpNode *exp = AR_EXP_FromExpression(record_map, ast_exp);

        // Find the resolved name of the entity - its alias, its identifier if referring to a full entity,
        // the entity.prop combination ("a.val"), or the function call ("MAX(a.val)").
        // The WITH clause requires that the resolved name be an alias or identifier.
        const char *identifier = NULL;
        const cypher_astnode_t *alias_node = cypher_ast_projection_get_alias(projection);
        if (alias_node) {
            // The projection either has an alias (AS), is a function call, or is a property specification (e.name).
            /// TODO should issue syntax failure in the latter 2 cases
            identifier = cypher_ast_identifier_get_name(alias_node);
        } else {
            // This expression did not have an alias, so it must be an identifier
            const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);
            assert(cypher_astnode_type(ast_exp) == CYPHER_AST_IDENTIFIER);
            // Retrieve "a" from "RETURN a" or "RETURN a AS e" (theoretically; the latter case is already handled)
            identifier = cypher_ast_identifier_get_name(ast_exp);
        }

        exp->resolved_name = identifier;

        with_expressions = array_append(with_expressions, exp);
    }

    return with_expressions;

}

AR_ExpNode** _BuildCallProjections(RecordMap *record_map, const cypher_astnode_t *call_clause) {
    // Handle yield entities
    uint yield_count = cypher_ast_call_nprojections(call_clause);
    AR_ExpNode **expressions = array_new(AR_ExpNode*, yield_count);

    for (uint i = 0; i < yield_count; i ++) {
        const cypher_astnode_t *projection = cypher_ast_call_get_projection(call_clause, i);
        const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);

        // Construction an AR_ExpNode to represent this entity.
        AR_ExpNode *exp = AR_EXP_FromExpression(record_map, ast_exp);

        const char *identifier = NULL;
        const cypher_astnode_t *alias_node = cypher_ast_projection_get_alias(projection);
        if (alias_node) {
            // The projection either has an alias (AS), is a function call, or is a property specification (e.name).
            identifier = cypher_ast_identifier_get_name(alias_node);
        } else {
            // This expression did not have an alias, so it must be an identifier
            const cypher_astnode_t *ast_exp = cypher_ast_projection_get_expression(projection);
            assert(cypher_astnode_type(ast_exp) == CYPHER_AST_IDENTIFIER);
            // Retrieve "a" from "RETURN a" or "RETURN a AS e" (theoretically; the latter case is already handled)
            identifier = cypher_ast_identifier_get_name(ast_exp);
        }

        exp->resolved_name = identifier;

        expressions = array_append(expressions, exp);
    }

    // If the procedure call is missing its yield part, include procedure outputs. 
    if (yield_count == 0) {
        const char *proc_name = cypher_ast_proc_name_get_value(cypher_ast_call_get_proc_name(call_clause));
        ProcedureCtx *proc = Proc_Get(proc_name);
        assert(proc);

        unsigned int output_count = array_len(proc->output);
        for (uint i = 0; i < output_count; i++) {
            const char *name = proc->output[i]->name;

            // TODO the 'name' variable doesn't have an AST ID, so an assertion in
            // AR_EXP_NewVariableOperandNode() fails without this call. Consider options.
            ASTMap_FindOrAddAlias(AST_GetFromTLS(), name, IDENTIFIER_NOT_FOUND);
            AR_ExpNode *exp = AR_EXP_NewVariableOperandNode(record_map, name, NULL);
            exp->resolved_name = name; // TODO kludge?
            expressions = array_append(expressions, exp);
        }
    }

    return expressions;
}

const char** _BuildCallArguments(RecordMap *record_map, const cypher_astnode_t *call_clause) {
    // Handle argument entities
    uint arg_count = cypher_ast_call_narguments(call_clause);
    // if (expressions == NULL) expressions = array_new(AR_ExpNode*, arg_count);
    const char **arguments = array_new(const char*, arg_count);
    for (uint i = 0; i < arg_count; i ++) {

        const cypher_astnode_t *ast_exp = cypher_ast_call_get_argument(call_clause, i);

        // Construction an AR_ExpNode to represent this entity.
        // AR_ExpNode *exp = AR_EXP_FromExpression(record_map, ast_exp);

        const cypher_astnode_t *identifier_node = cypher_ast_projection_get_alias(ast_exp);
        const char *identifier = cypher_ast_identifier_get_name(identifier_node);

        arguments = array_append(arguments, identifier);
        // expressions = array_append(expressions, exp);
    }

    return arguments;
}

/* Given an AST path, construct a series of scans and traversals to model it. */
void _ExecutionPlanSegment_BuildTraversalOps(ExecutionPlanSegment *segment, QueryGraph *qg, FT_FilterNode *ft, const cypher_astnode_t *path, Vector *traversals) {
    GraphContext *gc = GraphContext_GetFromTLS();
    AST *ast = AST_GetFromTLS();
    OpBase *op = NULL;

    uint nelems = cypher_ast_pattern_path_nelements(path);
    if (nelems == 1) {
        // Only one entity is specified - build a node scan.
        const cypher_astnode_t *ast_node = cypher_ast_pattern_path_get_element(path, 0);
        Node *n = QueryGraph_GetEntityByASTRef(qg, ast_node);
        uint ast_id = n->entity->id;
        uint rec_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);
        if(cypher_ast_node_pattern_nlabels(ast_node) > 0) {
            op = NewNodeByLabelScanOp(n, rec_idx);
        } else {
            op = NewAllNodeScanOp(gc->g, n, rec_idx);
        }
        Vector_Push(traversals, op);
        return;
    }

    // This path must be expressed with one or more traversals.
    size_t expCount = 0;
    AlgebraicExpression **exps = AlgebraicExpression_FromPath(segment->record_map, qg, path, &expCount);

    TRAVERSE_ORDER order;
    if (exps[0]->op == AL_EXP_UNARY) {
        // If either the first or last expression simply specifies a node, it should
        // be replaced by a label scan. (This can be the case after building a
        // variable-length traversal like MATCH (a)-[*]->(b:labeled)
        AlgebraicExpression *to_replace = exps[0];

        // Retrieve the AST ID for the source node
        uint ast_id = exps[0]->src_node->entity->id;
        // Convert to a Record ID
        uint record_id = RecordMap_FindOrAddID(segment->record_map, ast_id);

        op = NewNodeByLabelScanOp(to_replace->src_node, record_id);
        Vector_Push(traversals, op);
        AlgebraicExpression_Free(to_replace);
        for (uint q = 1; q < expCount; q ++) {
            exps[q-1] = exps[q];
        }
        expCount --;
        order = TRAVERSE_ORDER_FIRST;
    } else if (exps[expCount - 1]->op == AL_EXP_UNARY) {
        AlgebraicExpression *to_replace = exps[expCount - 1];

        // Retrieve the AST ID for the source node)
        uint ast_id = exps[0]->src_node->entity->id;
        // Convert to a Record ID
        uint record_id = RecordMap_FindOrAddID(segment->record_map, ast_id);
        op = NewNodeByLabelScanOp(to_replace->src_node, record_id);
        Vector_Push(traversals, op);
        AlgebraicExpression_Free(to_replace);
        expCount --;
        order = TRAVERSE_ORDER_LAST;
    } else {
        order = determineTraverseOrder(ft, exps, expCount);
    }

    if(order == TRAVERSE_ORDER_FIRST) {
        if (op == NULL) {
            // We haven't already built the appropriate label scan
            AlgebraicExpression *exp = exps[0];
            selectEntryPoint(exp, ft);

            // Retrieve the AST ID for the source node
            uint ast_id = exps[0]->src_node->entity->id;
            // Convert to a Record ID
            uint record_id = RecordMap_FindOrAddID(segment->record_map, ast_id);

            // Create SCAN operation.
            if(exp->src_node->label) {
                /* There's no longer need for the last matrix operand
                 * as it's been replaced by label scan. */
                AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
                op = NewNodeByLabelScanOp(exp->src_node, record_id);
                Vector_Push(traversals, op);
            } else {
                op = NewAllNodeScanOp(gc->g, exp->src_node, record_id);
                Vector_Push(traversals, op);
            }
        }
        for(int i = 0; i < expCount; i++) {
            if(exps[i]->operand_count == 0) continue;
            uint ast_id;
            uint src_node_idx;
            uint dest_node_idx;
            uint edge_idx = IDENTIFIER_NOT_FOUND;
            if (exps[i]->op == AL_EXP_UNARY) {
                // TODO ?
                // exps[i]->dest_node_idx = exps[i]->src_node_idx;
            } else {
                // Make sure that all entities are represented in Record
                uint ast_id = exps[i]->src_node->entity->id;
                src_node_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);

                ast_id = exps[i]->dest_node->entity->id;
                dest_node_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);

                if (exps[i]->edge) {
                    ast_id = exps[i]->edge->entity->id;
                    edge_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);
                }
            }
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             src_node_idx,
                                             dest_node_idx,
                                             gc->g);
            } else {
                op = NewCondTraverseOp(gc->g, exps[i], src_node_idx, dest_node_idx, edge_idx, TraverseRecordCap(ast));
            }
            Vector_Push(traversals, op);
        }
    } else {
        if (op == NULL) {
            // We haven't already built the appropriate label scan
            AlgebraicExpression *exp = exps[expCount-1];
            selectEntryPoint(exp, ft);

            // Retrieve the AST ID for the destination node
            uint ast_id = exps[0]->dest_node->entity->id;
            // Convert to a Record ID
            uint record_id = RecordMap_FindOrAddID(segment->record_map, ast_id);

            // Create SCAN operation.
            if(exp->dest_node->label) {
                /* There's no longer need for the last matrix operand
                 * as it's been replaced by label scan. */
                AlgebraicExpression_RemoveTerm(exp, exp->operand_count-1, NULL);
                op = NewNodeByLabelScanOp(exp->dest_node, record_id);
                Vector_Push(traversals, op);
            } else {
                op = NewAllNodeScanOp(gc->g, exp->dest_node, record_id);
                Vector_Push(traversals, op);
            }
        }

        for(int i = expCount-1; i >= 0; i--) {
            if(exps[i]->operand_count == 0) continue;
            AlgebraicExpression_Transpose(exps[i]);
            // TODO tmp
            uint ast_id;
            uint src_node_idx;
            uint dest_node_idx;
            uint edge_idx = IDENTIFIER_NOT_FOUND;
            if (exps[i]->op == AL_EXP_UNARY) {
                // exps[i]->src_node_idx = exps[i]->dest_node_idx;
            } else {
                // Make sure that all entities are represented in Record
                uint ast_id = exps[i]->src_node->entity->id;
                src_node_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);

                ast_id = exps[i]->dest_node->entity->id;
                dest_node_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);

                if (exps[i]->edge) {
                    ast_id = exps[i]->edge->entity->id;
                    edge_idx = RecordMap_FindOrAddID(segment->record_map, ast_id);
                }
            }
            if(exps[i]->minHops != 1 || exps[i]->maxHops != 1) {
                op = NewCondVarLenTraverseOp(exps[i],
                                             exps[i]->minHops,
                                             exps[i]->maxHops,
                                             src_node_idx,
                                             dest_node_idx,
                                             gc->g);
            } else {
                op = NewCondTraverseOp(gc->g, exps[i], src_node_idx, dest_node_idx, edge_idx, TraverseRecordCap(ast));
            }
            Vector_Push(traversals, op);
        }
    }
    // Free the expressions array, as its parts have been converted into operations
    free(exps);
}

void _ExecutionPlanSegment_AddTraversalOps(Vector *ops, OpBase *cartesian_root, Vector *traversals) {
    if(cartesian_root) {
        // If we're traversing multiple disjoint paths, the new traversal
        // should be connected uner a Cartesian product.
        OpBase *childOp;
        OpBase *parentOp;
        Vector_Pop(traversals, &parentOp);
        // Connect cartesian product to the root of traversal.
        ExecutionPlan_AddOp(cartesian_root, parentOp);
        while(Vector_Pop(traversals, &childOp)) {
            ExecutionPlan_AddOp(parentOp, childOp);
            parentOp = childOp;
        }
    } else {
        // Otherwise, the traversals can be added sequentially to the overall ops chain
        OpBase *op;
        for(int traversalIdx = 0; traversalIdx < Vector_Size(traversals); traversalIdx++) {
            Vector_Get(traversals, traversalIdx, &op);
            Vector_Push(ops, op);
        }
    }
}

// Map the required AST entities and build expressions to match
// the AST slice's WITH, RETURN, and ORDER clauses
void _ExecutionPlanSegment_BuildProjections(ExecutionPlanSegment *segment, AST *ast) {
    // Retrieve a RETURN clause if one is specified in this AST's range
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    // Retrieve a WITH clause if one is specified in this AST's range
    const cypher_astnode_t *with_clause = AST_GetClause(ast, CYPHER_AST_WITH);
    // We cannot have both a RETURN and WITH clause
    assert(!(ret_clause && with_clause));
    segment->projections = NULL;
    segment->order_expressions = NULL;

    const cypher_astnode_t *order_clause = NULL;
    if (ret_clause) {
        segment->projections = _BuildReturnExpressions(segment->record_map, ret_clause);
        order_clause = cypher_ast_return_get_order_by(ret_clause);
    } else if (with_clause) {
        segment->projections = _BuildWithExpressions(segment->record_map, with_clause);
        order_clause = cypher_ast_with_get_order_by(with_clause);
    }

    if (order_clause) segment->order_expressions = _BuildOrderExpressions(segment->record_map, segment->projections, order_clause);

    // const cypher_astnode_t *call_clause = AST_GetClause(ast, CYPHER_AST_CALL);
    // if(call_clause) {
        // segment->projections = _BuildCallExpressions(segment->record_map, segment->projections, call_clause);
    // }

}

// TODO tmp, replace with better logic
void _initOpRecordMap(OpBase *op, RecordMap *record_map) {
    if (op == NULL) return;
    op->record_map = record_map;
    for (uint i = 0; i < op->childCount; i ++) {
        _initOpRecordMap(op->children[i], record_map);
    }
}

ExecutionPlanSegment* _NewExecutionPlanSegment(RedisModuleCtx *ctx, GraphContext *gc, AST *ast, ResultSet *result_set, AR_ExpNode **prev_projections, OpBase *prev_op) {

    // Allocate a new segment
    ExecutionPlanSegment *segment = rm_malloc(sizeof(ExecutionPlanSegment));

    // Initialize map of Record IDs
    RecordMap *record_map = RecordMap_New();
    segment->record_map = record_map;

    if (prev_projections) {
        // We have an array of identifiers provided by a prior WITH clause -
        // these will correspond to our first Record entities
        uint projection_count = array_len(prev_projections);
        for (uint i = 0; i < projection_count; i++) {
            AR_ExpNode *projection = prev_projections[i];
            RecordMap_FindOrAddAlias(record_map, projection->resolved_name);
        }
    }

    // Build projections from this AST's WITH, RETURN, and ORDER clauses
    _ExecutionPlanSegment_BuildProjections(segment, ast);

    Vector *ops = NewVector(OpBase*, 1);

    // Build query graph
    QueryGraph *qg = BuildQueryGraph(gc, ast);
    segment->query_graph = qg;

    // Build filter tree
    FT_FilterNode *filter_tree = AST_BuildFilterTree(ast, record_map);
    segment->filter_tree = filter_tree;


    const cypher_astnode_t *call_clause = AST_GetClause(ast, CYPHER_AST_CALL);
    if(call_clause) {
        // A call clause has a procedure name, 0+ arguments (parenthesized expressions), and a projection if YIELD is included
        const char *proc_name = cypher_ast_proc_name_get_value(cypher_ast_call_get_proc_name(call_clause));
        const char **arguments = _BuildCallArguments(record_map, call_clause);
        AR_ExpNode **yield_exps = _BuildCallProjections(record_map, call_clause);
        uint yield_count = array_len(yield_exps);
        const char **yields = array_new(const char *, yield_count);
        if (segment->projections == NULL) segment->projections = array_new(AR_ExpNode*, yield_count);
        uint *call_modifies = array_new(uint, yield_count);
        for (uint i = 0; i < yield_count; i ++) {
            // TODO revisit this
            // Add yielded expressions to segment projections.
            segment->projections = array_append(segment->projections, yield_exps[i]);
            // Track the names of yielded variables.
            yields = array_append(yields, yield_exps[i]->resolved_name);
            // Track which variables are modified by this operation.
            call_modifies = array_append(call_modifies, yield_exps[i]->operand.variadic.entity_alias_idx);
        }

        OpBase *opProcCall = NewProcCallOp(proc_name, arguments, yields, call_modifies);
        Vector_Push(ops, opProcCall);
    }

    const cypher_astnode_t **match_clauses = AST_CollectReferencesInRange(ast, CYPHER_AST_MATCH);
    uint match_count = array_len(match_clauses);

    /* TODO Currently, we don't differentiate between:
     * MATCH (a) MATCH (b)
     * and
     * MATCH (a), (b)
     * Introduce this distinction. */
    OpBase *cartesianProduct = NULL;
    if (match_count > 1) {
        cartesianProduct = NewCartesianProductOp();
        Vector_Push(ops, cartesianProduct);
    }

    // Build traversal operations for every MATCH clause
    for (uint i = 0; i < match_count; i ++) {
        // Each MATCH clause has a pattern that consists of 1 or more paths
        const cypher_astnode_t *ast_pattern = cypher_ast_match_get_pattern(match_clauses[i]);
        uint npaths = cypher_ast_pattern_npaths(ast_pattern);

        /* If we're dealing with multiple paths (which our validations have guaranteed
         * are disjoint), we'll join them all together with a Cartesian product (full join). */
        if ((cartesianProduct == NULL) && (cypher_ast_pattern_npaths(ast_pattern) > 1)) {
            cartesianProduct = NewCartesianProductOp();
            Vector_Push(ops, cartesianProduct);
        }

        Vector *path_traversal = NewVector(OpBase*, 1);
        for (uint j = 0; j < npaths; j ++) {
            // Convert each path into the appropriate traversal operation(s).
            const cypher_astnode_t *path = cypher_ast_pattern_get_path(ast_pattern, j);
            _ExecutionPlanSegment_BuildTraversalOps(segment, qg, filter_tree, path, path_traversal);
            _ExecutionPlanSegment_AddTraversalOps(ops, cartesianProduct, path_traversal);
            Vector_Clear(path_traversal);
        }
        Vector_Free(path_traversal);
    }

    array_free(match_clauses);

    // Set root operation
    const cypher_astnode_t *unwind_clause = AST_GetClause(ast, CYPHER_AST_UNWIND);
    if(unwind_clause) {
        AST_UnwindContext unwind_ast_ctx = AST_PrepareUnwindOp(unwind_clause, record_map);

        OpBase *opUnwind = NewUnwindOp(unwind_ast_ctx.record_idx, unwind_ast_ctx.exps);
        Vector_Push(ops, opUnwind);
    }

    bool create_clause = AST_ContainsClause(ast, CYPHER_AST_CREATE);
    if(create_clause) {
        AST_CreateContext create_ast_ctx = AST_PrepareCreateOp(record_map, ast, qg);
        OpBase *opCreate = NewCreateOp(&result_set->stats,
                                       create_ast_ctx.nodes_to_create,
                                       create_ast_ctx.edges_to_create);
        Vector_Push(ops, opCreate);
    }

    const cypher_astnode_t *merge_clause = AST_GetClause(ast, CYPHER_AST_MERGE);
    if(merge_clause) {
        // A merge clause provides a single path that must exist or be created.
        // As with paths in a MATCH query, build the appropriate traversal operations
        // and append them to the set of ops.
        const cypher_astnode_t *path = cypher_ast_merge_get_pattern_path(merge_clause);
        Vector *path_traversal = NewVector(OpBase*, 1);
        _ExecutionPlanSegment_BuildTraversalOps(segment, qg, filter_tree, path, path_traversal);
        _ExecutionPlanSegment_AddTraversalOps(ops, NULL, path_traversal);
        Vector_Free(path_traversal);

        // Append a merge operation
        AST_MergeContext merge_ast_ctx = AST_PrepareMergeOp(record_map, ast, merge_clause, qg);
        OpBase *opMerge = NewMergeOp(&result_set->stats,
                                     merge_ast_ctx.nodes_to_merge,
                                     merge_ast_ctx.edges_to_merge);
        Vector_Push(ops, opMerge);
    }

    const cypher_astnode_t *delete_clause = AST_GetClause(ast, CYPHER_AST_DELETE);
    if(delete_clause) {
        uint *nodes_ref;
        uint *edges_ref;
        AST_PrepareDeleteOp(delete_clause, record_map, &nodes_ref, &edges_ref);
        OpBase *opDelete = NewDeleteOp(nodes_ref, edges_ref, &result_set->stats);
        Vector_Push(ops, opDelete);
    }

    const cypher_astnode_t *set_clause = AST_GetClause(ast, CYPHER_AST_SET);
    if(set_clause) {
        // Create a context for each update expression.
        uint nitems;
        EntityUpdateEvalCtx *update_exps = AST_PrepareUpdateOp(set_clause, record_map, &nitems);
        OpBase *op_update = NewUpdateOp(gc, update_exps, nitems, &result_set->stats);
        Vector_Push(ops, op_update);
    }

    const cypher_astnode_t *with_clause = AST_GetClause(ast, CYPHER_AST_WITH);
    const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);

    assert(!(with_clause && ret_clause));

    uint *modifies = NULL;

    // WITH/RETURN projections have already been constructed from the AST
    AR_ExpNode **projections = segment->projections;

    if (with_clause || ret_clause || call_clause) {
        uint exp_count = array_len(projections);
        modifies = array_new(uint, exp_count);
        for (uint i = 0; i < exp_count; i ++) {
            AR_ExpNode *exp = projections[i];
            // TODO 
            uint exp_id = exp->operand.variadic.entity_alias_idx;
            // if (exp->type == AR_EXP_OPERAND && exp->operand.type == AR_EXP_VARIADIC) {
            
            // }
            // uint exp_id = RecordMap_ExpressionToRecordID(segment->record_map, exp);
            modifies = array_append(modifies, exp_id);
        }
    }

    OpBase *op;

    if(with_clause) {
        // uint *with_projections = AST_WithClauseModifies(ast, with_clause);
        if (AST_ClauseContainsAggregation(with_clause)) {
            op = NewAggregateOp(projections, modifies);
        } else {
            op = NewProjectOp(projections, modifies);
        }
        Vector_Push(ops, op);

        if (cypher_ast_with_is_distinct(with_clause)) {
            op = NewDistinctOp();
            Vector_Push(ops, op);
        }

        const cypher_astnode_t *skip_clause = cypher_ast_with_get_skip(with_clause);
        const cypher_astnode_t *limit_clause = cypher_ast_with_get_limit(with_clause);

        uint skip = 0;
        uint limit = 0;
        if (skip_clause) skip = AST_ParseIntegerNode(skip_clause);
        if (limit_clause) limit = AST_ParseIntegerNode(limit_clause);

        if (segment->order_expressions) {
            const cypher_astnode_t *order_clause = cypher_ast_with_get_order_by(with_clause);
            int direction = AST_PrepareSortOp(order_clause);
            // The sort operation will obey a specified limit, but must account for skipped records
            uint sort_limit = (limit > 0) ? limit + skip : 0;
            op = NewSortOp(segment->order_expressions, direction, sort_limit);
            Vector_Push(ops, op);
        }

        if (skip_clause) {
            OpBase *op_skip = NewSkipOp(skip);
            Vector_Push(ops, op_skip);
        }

        if (limit_clause) {
            OpBase *op_limit = NewLimitOp(limit);
            Vector_Push(ops, op_limit);
        }
    } else if (ret_clause) {

        // TODO we may not need a new project op if the query is something like:
        // MATCH (a) WITH a.val AS val RETURN val
        // Though we would still need a new projection (barring later optimizations) for:
        // MATCH (a) WITH a.val AS val RETURN val AS e
        if (AST_ClauseContainsAggregation(ret_clause)) {
            op = NewAggregateOp(projections, modifies);
        } else {
            op = NewProjectOp(projections, modifies);
        }
        Vector_Push(ops, op);

        if (cypher_ast_return_is_distinct(ret_clause)) {
            op = NewDistinctOp();
            Vector_Push(ops, op);
        }

        const cypher_astnode_t *order_clause = cypher_ast_return_get_order_by(ret_clause);
        const cypher_astnode_t *skip_clause = cypher_ast_return_get_skip(ret_clause);
        const cypher_astnode_t *limit_clause = cypher_ast_return_get_limit(ret_clause);

        uint skip = 0;
        uint limit = 0;
        if (skip_clause) skip = AST_ParseIntegerNode(skip_clause);
        if (limit_clause) limit = AST_ParseIntegerNode(limit_clause);

        if (segment->order_expressions) {
            int direction = AST_PrepareSortOp(order_clause);
            // The sort operation will obey a specified limit, but must account for skipped records
            uint sort_limit = (limit > 0) ? limit + skip : 0;
            op = NewSortOp(segment->order_expressions, direction, sort_limit);
            Vector_Push(ops, op);
        }

        if (skip_clause) {
            OpBase *op_skip = NewSkipOp(skip);
            Vector_Push(ops, op_skip);
        }

        if (limit_clause) {
            OpBase *op_limit = NewLimitOp(limit);
            Vector_Push(ops, op_limit);
        }

        op = NewResultsOp(result_set, qg);
        Vector_Push(ops, op);
    } else if (call_clause) {
        op = NewResultsOp(result_set, qg);
        Vector_Push(ops, op);
    }

    OpBase *parent_op;
    OpBase *child_op;
    Vector_Pop(ops, &parent_op);
    segment->root = parent_op;

    while(Vector_Pop(ops, &child_op)) {
        ExecutionPlan_AddOp(parent_op, child_op);
        parent_op = child_op;
    }

    Vector_Free(ops);

    if (prev_op) {
        // Need to connect this segment to the previous one.
        // If the last operation of this segment is a potential data producer, join them
        // under an Apply operation.
        if (parent_op->type & OP_TAPS) {
            OpBase *op_apply = NewApplyOp();
            ExecutionPlan_PushBelow(parent_op, op_apply);
            ExecutionPlan_AddOp(op_apply, prev_op);
        } else {
            // All operations can be connected in a single chain.
            ExecutionPlan_AddOp(parent_op, prev_op);
        }
    }

    if(segment->filter_tree) {
        Vector *sub_trees = FilterTree_SubTrees(segment->filter_tree);

        /* For each filter tree find the earliest position along the execution
         * after which the filter tree can be applied. */
        for(int i = 0; i < Vector_Size(sub_trees); i++) {
            FT_FilterNode *tree;
            Vector_Get(sub_trees, i, &tree);

            uint *references = FilterTree_CollectModified(tree);

            /* Scan execution segment, locate the earliest position where all
             * references been resolved. */
            OpBase *op = ExecutionPlan_LocateReferences(segment->root, references);
            assert(op);

            /* Create filter node.
             * Introduce filter op right below located op. */
            OpBase *filter_op = NewFilterOp(tree);
            ExecutionPlan_PushBelow(op, filter_op);
            array_free(references);
        }
        Vector_Free(sub_trees);
    }

    // TODO tmp, replace with better logic
    _initOpRecordMap(segment->root, segment->record_map);

    return segment;
}

ExecutionPlan* NewExecutionPlan(RedisModuleCtx *ctx, GraphContext *gc, bool compact, bool explain) {
    AST *ast = AST_GetFromTLS();

    ExecutionPlan *plan = rm_malloc(sizeof(ExecutionPlan));

    plan->result_set = _prepare_resultset(ctx, ast, compact);

    uint with_clause_count = AST_GetClauseCount(ast, CYPHER_AST_WITH);
    plan->segment_count = with_clause_count + 1;

    plan->segments = rm_malloc(plan->segment_count * sizeof(ExecutionPlanSegment));

    uint *segment_indices = NULL;
    if (with_clause_count > 0) segment_indices = AST_GetClauseIndices(ast, CYPHER_AST_WITH);

    uint i = 0;
    uint end_offset;
    uint start_offset = 0;
    OpBase *prev_op = NULL;
    ExecutionPlanSegment *segment = NULL;
    AR_ExpNode **input_projections = NULL;

    // The original AST does not need to be modified if our query only has one segment
    AST *ast_segment = ast;
    if (with_clause_count > 0) {
        for (i = 0; i < with_clause_count; i++) {
            end_offset = segment_indices[i] + 1; // Switching from index to bound, so add 1
            ast_segment = AST_NewSegment(ast, start_offset, end_offset);
            segment =_NewExecutionPlanSegment(ctx, gc, ast_segment, plan->result_set, input_projections, prev_op);
            plan->segments[i] = segment;
            // TODO probably a memory leak on ast->root
            AST_Free(ast_segment); // Free all AST constructions scoped to this segment
            // Store the expressions constructed by this segment's WITH projection to pass into the *next* segment
            prev_op = segment->root;
            input_projections = segment->projections;
            start_offset = end_offset;
        }
        // Prepare the last AST segment
        end_offset = cypher_astnode_nchildren(ast->root);
        ast_segment = AST_NewSegment(ast, start_offset, end_offset);
    }

    segment = _NewExecutionPlanSegment(ctx, gc, ast_segment, plan->result_set, input_projections, prev_op);
    plan->segments[i] = segment;

    plan->root = plan->segments[i]->root;

    optimizePlan(gc, plan);


    // const cypher_astnode_t *ret_clause = AST_GetClause(ast, CYPHER_AST_RETURN);
    // TODO what is happening here?
    AR_ExpNode **return_columns = segment->projections;
    if (segment && segment->projections) {
        return_columns = segment->projections; // TODO kludge
        plan->result_set->column_count = array_len(return_columns);
    }
    if (explain == false) {
        plan->result_set->exps = segment->projections;
        ResultSet_ReplyWithPreamble(plan->result_set, return_columns);
    }
    // Free current AST segment if it has been constructed here.
    if (ast_segment != ast) {
        AST_Free(ast_segment);
    }
    // _AST_Free(ast);

    return plan;
}

void _ExecutionPlan_Print(const OpBase *op, RedisModuleCtx *ctx, char *buffer, int ident, int *op_count) {
    if(!op) return;

    *op_count += 1; // account for current operation.

    // Construct operation string representation.
    int len = sprintf(buffer, "%*s%s", ident, "", op->name);

    RedisModule_ReplyWithStringBuffer(ctx, buffer, len);

    // Recurse over child operations.
    for(int i = 0; i < op->childCount; i++) {
        _ExecutionPlan_Print(op->children[i], ctx, buffer, ident+4, op_count);
    }
}

// Replys with a string representation of given execution plan.
void ExecutionPlan_Print(const ExecutionPlan *plan, RedisModuleCtx *ctx) {
    assert(plan && ctx);

    int op_count = 0;   // Number of operations printed.
    char buffer[1024];

    // No idea how many operation are in execution plan.
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    _ExecutionPlan_Print(plan->root, ctx, buffer, 0, &op_count);
    RedisModule_ReplySetArrayLength(ctx, op_count);
}

void _ExecutionPlanInit(OpBase *root) {
    if(root->init) root->init(root);
    for(int i = 0; i < root->childCount; i++) {
        _ExecutionPlanInit(root->children[i]);
    }
}

void ExecutionPlanInit(ExecutionPlan *plan) {
    if(!plan) return;
    _ExecutionPlanInit(plan->root);
}


ResultSet* ExecutionPlan_Execute(ExecutionPlan *plan) {
    Record r;
    OpBase *op = plan->root;

    ExecutionPlanInit(plan);
    while((r = op->consume(op)) != NULL) Record_Free(r);
    return plan->result_set;
}



void _ExecutionPlan_FreeOperations(OpBase* op) {
    for(int i = 0; i < op->childCount; i++) {
        _ExecutionPlan_FreeOperations(op->children[i]);
    }
    OpBase_Free(op);
}

void ExecutionPlan_Free(ExecutionPlan *plan) {
    if(plan == NULL) return;
    _ExecutionPlan_FreeOperations(plan->root);

    for (uint i = 0; i < plan->segment_count; i ++) {
        ExecutionPlanSegment *segment = plan->segments[i];
        QueryGraph_Free(segment->query_graph);
        rm_free(segment);
    }
    rm_free(plan->segments);

    rm_free(plan);
}
