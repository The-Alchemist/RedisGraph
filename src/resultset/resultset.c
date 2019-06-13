/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "resultset.h"
#include "../value.h"
#include "../util/arr.h"
#include "../util/rmalloc.h"
#include "../grouping/group_cache.h"
#include "../arithmetic/aggregate.h"

// Choose the appropriate reply formatter
EmitRecordFunc _ResultSet_SetReplyFormatter(bool compact) {
    if (compact) return ResultSet_EmitCompactRecord;
    return ResultSet_EmitVerboseRecord;
}

static void _ResultSet_ReplayStats(RedisModuleCtx* ctx, ResultSet* set) {
    char buff[512] = {0};
    size_t resultset_size = 1; /* query execution time. */
    int buflen;

    if(set->stats.labels_added > 0) resultset_size++;
    if(set->stats.nodes_created > 0) resultset_size++;
    if(set->stats.properties_set > 0) resultset_size++;
    if(set->stats.relationships_created > 0) resultset_size++;
    if(set->stats.nodes_deleted > 0) resultset_size++;
    if(set->stats.relationships_deleted > 0) resultset_size++;

    RedisModule_ReplyWithArray(ctx, resultset_size);

    if(set->stats.labels_added > 0) {
        buflen = sprintf(buff, "Labels added: %d", set->stats.labels_added);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }

    if(set->stats.nodes_created > 0) {
        buflen = sprintf(buff, "Nodes created: %d", set->stats.nodes_created);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }

    if(set->stats.properties_set > 0) {
        buflen = sprintf(buff, "Properties set: %d", set->stats.properties_set);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }

    if(set->stats.relationships_created > 0) {
        buflen = sprintf(buff, "Relationships created: %d", set->stats.relationships_created);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }

    if(set->stats.nodes_deleted > 0) {
        buflen = sprintf(buff, "Nodes deleted: %d", set->stats.nodes_deleted);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }

    if(set->stats.relationships_deleted > 0) {
        buflen = sprintf(buff, "Relationships deleted: %d", set->stats.relationships_deleted);
        RedisModule_ReplyWithStringBuffer(ctx, (const char*)buff, buflen);
    }
}

static void _ResultSet_ReplyWithHeader(ResultSet *set, Record r) {
    assert(set->recordCount == 0);

    set->column_count = array_len(set->exps);

    /* Replay with table header. */
    if (set->compact) {
        ResultSet_ReplyWithCompactHeader(set->ctx, set->exps, r);
    } else {
        ResultSet_ReplyWithVerboseHeader(set->ctx, set->exps); 
    }
}

ResultSet* NewResultSet(RedisModuleCtx *ctx, bool distinct, bool compact) {
    ResultSet* set = (ResultSet*)malloc(sizeof(ResultSet));
    set->ctx = ctx;
    set->gc = GraphContext_GetFromTLS();
    set->distinct = distinct;
    set->compact = compact;
    set->EmitRecord = _ResultSet_SetReplyFormatter(set->compact);
    set->recordCount = 0;    
    set->column_count = 0; // TODO necessary variable?

    set->stats.labels_added = 0;
    set->stats.nodes_created = 0;
    set->stats.properties_set = 0;
    set->stats.relationships_created = 0;
    set->stats.nodes_deleted = 0;
    set->stats.relationships_deleted = 0;

    return set;
}

// Initialize the user-facing reply arrays.
void ResultSet_ReplyWithPreamble(ResultSet *set, AR_ExpNode **exps) {
    if (exps == NULL || array_len(exps) == 0) {
        // Queries that don't form result sets will only emit statistics
        RedisModule_ReplyWithArray(set->ctx, 1);
        return;
    }

    // header, records, statistics
    RedisModule_ReplyWithArray(set->ctx, 3);

    // _ResultSet_ReplyWithHeader(set, exps);

    // // We don't know at this point the number of records we're about to return.
    // RedisModule_ReplyWithArray(set->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
}

int ResultSet_AddRecord(ResultSet *set, Record r) {
    // TODO tmp, think
    if (set->recordCount == 0) {
        // Replay header here for the moment
        _ResultSet_ReplyWithHeader(set, r);

        // We don't know at this point the number of records we're about to return.
        RedisModule_ReplyWithArray(set->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    }
    set->recordCount++;

    // Output the current record using the defined formatter
    set->EmitRecord(set->ctx, set->gc, r, set->column_count);

    return RESULTSET_OK;
}

void ResultSet_Replay(ResultSet* set) {
    // If we have emitted records, set the number of elements in the
    // preceding array
    if (set->column_count > 0) {
        RedisModule_ReplySetArrayLength(set->ctx, set->recordCount);
    }
    _ResultSet_ReplayStats(set->ctx, set);
}

void ResultSet_Free(ResultSet *set) {
    if(!set) return;

    free(set);
}
