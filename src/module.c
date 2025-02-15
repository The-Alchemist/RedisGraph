/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <unistd.h>
#include <assert.h>
#include "redismodule.h"
#include "config.h"
#include "version.h"
#include "redisearch_api.h"
#include "commands/commands.h"
#include "util/thpool/thpool.h"
#include "arithmetic/agg_funcs.h"
#include "procedures/procedure.h"
#include "arithmetic/arithmetic_expression.h"
#include "graph/serializers/graphcontext_type.h"

/* Thread pool. */
threadpool _thpool = NULL;
pthread_key_t _tlsGCKey;    // Thread local storage graph context key.

// Define the C symbols for RediSearch.
REDISEARCH_API_INIT_SYMBOLS();

/* Set up thread pool,
 * number of threads within pool should be
 * the number of available hyperthreads.
 * Returns 1 if thread pool initialized, 0 otherwise. */
int _Setup_ThreadPOOL(int threadCount) {
    // Create thread pool.
    _thpool = thpool_init(threadCount);
    if(_thpool == NULL) return 0;

    int error = pthread_key_create(&_tlsGCKey, NULL);
    if(error) {
        printf("Failed to create thread local storage key.\n");
        return 0;
    }
    return 1;
}

int _RegisterDataTypes(RedisModuleCtx *ctx) {
    if(GraphContextType_Register(ctx) == REDISMODULE_ERR) {
        printf("Failed to register GraphContext type\n");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    /* TODO: when module unloads call GrB_finalize. */
    assert(GrB_init(GrB_NONBLOCKING) == GrB_SUCCESS);
    GxB_set(GxB_FORMAT, GxB_BY_ROW); // all matrices in CSR format
    GxB_set(GxB_HYPER, GxB_NEVER_HYPER); // matrices are never hypersparse

    if (RedisModule_Init(ctx, "graph", REDISGRAPH_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // Make sure RediSearch is loaded.
    // if(RediSearch_Initialize() == REDISMODULE_OK) {
    //     /* Enable full-text search.
    //      * TODO: currently all procedure deal with full text-search 
    //      * once additional procedure will be introduce 
    //      * this registration invocation will have to change. */
    //     Proc_Register();
    // } else {
    //     RedisModule_Log(ctx, "warning", "RediSearch is missing, disabeling full-text search.");
    // }
    
    Proc_Register();
    AR_RegisterFuncs();     // Register arithmetic functions.
    Agg_RegisterFuncs();    // Register aggregation functions.

    long long threadCount = Config_GetThreadCount(ctx, argv, argc);
    if (!_Setup_ThreadPOOL(threadCount)) return REDISMODULE_ERR;
    RedisModule_Log(ctx, "notice", "Thread pool created, using %d threads.", threadCount);

    if (_RegisterDataTypes(ctx) != REDISMODULE_OK) return REDISMODULE_ERR;

    if(RedisModule_CreateCommand(ctx, "graph.QUERY", MGraph_Query, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(RedisModule_CreateCommand(ctx, "graph.DELETE", MGraph_Delete, "write", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(RedisModule_CreateCommand(ctx, "graph.EXPLAIN", MGraph_Explain, "write", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if(RedisModule_CreateCommand(ctx, "graph.BULK", MGraph_BulkInsert, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
