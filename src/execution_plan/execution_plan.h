/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __EXECUTION_PLAN_H__
#define __EXECUTION_PLAN_H__

#include "./ops/op.h"
#include "../graph/graph.h"
#include "../resultset/resultset.h"
#include "../filter_tree/filter_tree.h"


/* StreamState
 * Different states in which stream can be at. */
typedef enum {
    StreamUnInitialized,
    StreamConsuming,
    StreamDepleted,
} StreamState;

typedef struct {
    OpBase *root;                    // Root operation of this specific segment.
    QueryGraph *query_graph;         // QueryGraph representing all graph entities in this segment.
    FT_FilterNode *filter_tree;      // FilterTree containing filters to be applied to this segment.
    AR_ExpNode **projections;        // Expressions to be constructed for a WITH or RETURN clause.
    AR_ExpNode **order_expressions;  // Expressions to be constructed for an ORDER clause.
    TrieMap *record_map;
    uint record_len;                 // Length of Record being modified by this segment.
} ExecutionPlanSegment;

typedef struct {
    OpBase *root;                    // Root operation of overall ExecutionPlan.
    ExecutionPlanSegment **segments; // TODO might be able to refactor to remove this element
    ResultSet *result_set;           // ResultSet populated by this query
    uint segment_count;              // Number of segments in query (remove when possible)
} ExecutionPlan;

/* execution_plan_modify.c
 * Helper functions to move and analyze operations in an ExecutionPlan. */

/* Removes operation from execution plan. */
void ExecutionPlan_RemoveOp(ExecutionPlan *plan, OpBase *op);

/* Adds operation to execution plan as a child of parent. */
void ExecutionPlan_AddOp(OpBase *parent, OpBase *newOp);

/* Push b right below a. */
void ExecutionPlan_PushBelow(OpBase *a, OpBase *b);

/* Replace a with b. */
void ExecutionPlan_ReplaceOp(ExecutionPlan *plan, OpBase *a, OpBase *b);

/* Locates all operation which generate data. 
 * SCAN, UNWIND, PROCEDURE_CALL, CREATE. */
void ExecutionPlan_LocateTaps(OpBase *root, OpBase ***taps);

/* Locate the first operation of a given type within execution plan.
 * Returns NULL if operation wasn't found. */
OpBase* ExecutionPlan_LocateOp(OpBase *root, OPType type);

/* Returns an array of taps; operations which generate data
 * e.g. SCAN operations */
void ExecutionPlan_Taps(OpBase *root, OpBase ***taps);

/* Find the earliest operation on the ExecutionPlan at which all
 * references are resolved. */
OpBase* ExecutionPlan_LocateReferences(OpBase *root, uint *references);

/* execution_plan.c */

/* Creates a new execution plan from AST */
ExecutionPlan* NewExecutionPlan (
    RedisModuleCtx *ctx,    // Module-level context
    GraphContext *gc,       // Graph access and schemas
    bool compact,
    bool explain
);

/* Prints execution plan. */
char* ExecutionPlan_Print(const ExecutionPlan *plan);

/* Executes plan */
ResultSet* ExecutionPlan_Execute(ExecutionPlan *plan);

/* Free execution plan */
void ExecutionPlan_Free(ExecutionPlan *plan);

#endif