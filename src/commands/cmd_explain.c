/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "cmd_explain.h"
#include "../index/index.h"
#include "../util/rmalloc.h"
#include "../execution_plan/execution_plan.h"

extern pthread_key_t _tlsASTKey;  // Thread local storage AST key.
extern pthread_key_t _tlsGCKey;    // Thread local storage graph context key.

GraphContext* _empty_graph_context() {
    GraphContext *gc = NULL;
    gc = rm_malloc(sizeof(GraphContext));
    gc->g = Graph_New(1, 1);
    gc->index_count = 0;
    gc->attributes = NULL;
    gc->node_schemas = NULL;
    gc->string_mapping = NULL;
    gc->relation_schemas = NULL;
    gc->graph_name = rm_strdup("");

    pthread_setspecific(_tlsGCKey, gc);
    return gc;
}

/* Builds an execution plan but does not execute it
 * reports plan back to the client
 * Args:
 * argv[1] graph name [optional]
 * argv[2] query */
int MGraph_Explain(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc < 2) return RedisModule_WrongArity(ctx);

    const char *query;
    const char *graphname = NULL;
    bool free_graph_ctx = false;

    if(argc == 2) {
        query = RedisModule_StringPtrLen(argv[1], NULL);
    } else {
        graphname = RedisModule_StringPtrLen(argv[1], NULL);
        query = RedisModule_StringPtrLen(argv[2], NULL);
    }

    /* Parse query, get AST. */
    GraphContext *gc = NULL;
    ExecutionPlan *plan = NULL;

    cypher_parse_result_t *parse_result = cypher_parse(query, NULL, NULL, CYPHER_PARSE_ONLY_STATEMENTS);
    AST *ast = AST_Build(parse_result);

    pthread_setspecific(_tlsASTKey, ast);

    // Retrieve the GraphContext and acquire a read lock.
    gc = GraphContext_Retrieve(ctx, graphname);
    if(!gc) {
        RedisModule_ReplyWithError(ctx, "key doesn't contains a graph object.");
        goto cleanup;
    }

    // Perform query validations before and after ModifyAST
    if(AST_PerformValidations(ctx, ast) != AST_VALID) return REDISMODULE_OK;

    // TODO index ops
    // if (ast[0]->indexNode != NULL) { // index operation
        // char *reply = (ast[0]->indexNode->operation == CREATE_INDEX) ? "Create Index" : "Drop Index";
        // RedisModule_ReplyWithSimpleString(ctx, reply);
        // goto cleanup;
    // }

    // Retrieve the GraphContext and acquire a read lock.
    if(graphname) {
        gc = GraphContext_Retrieve(ctx, graphname);
        if(!gc) {
            RedisModule_ReplyWithError(ctx, "key doesn't contains a graph object.");
            goto cleanup;
        }
    } else {
        free_graph_ctx = true;
        gc = _empty_graph_context();
    }

    Graph_AcquireReadLock(gc->g);
    plan = NewExecutionPlan(ctx, gc, false, true);
    ExecutionPlan_Print(plan, ctx);

cleanup:
    if(plan) {
        Graph_ReleaseLock(gc->g);
        ExecutionPlan_Free(plan);
    }
    if(parse_result) cypher_parse_result_free(parse_result);
    if(free_graph_ctx) GraphContext_Free(gc);
    return REDISMODULE_OK;
}
