/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef __GRAPH_RESULTSET_H__
#define __GRAPH_RESULTSET_H__

#include "resultset_formatters.h"
#include "resultset_statistics.h"
#include "../redismodule.h"
#include "../execution_plan/record.h"
#include "../util/triemap/triemap.h"

#define RESULTSET_UNLIMITED 0
#define RESULTSET_OK 1
#define RESULTSET_FULL 0

typedef struct {
    RedisModuleCtx *ctx;
    GraphContext *gc;           /* Context used for mapping attribute strings and IDs */
    uint column_count; // TODO necessary variable?
    AR_ExpNode **exps;
    bool distinct;              /* Whether or not each record is unique. */
    bool compact;               /* Whether records should be returned in compact form. */
    size_t recordCount;         /* Number of records introduced. */
    ResultSetStatistics stats;  /* ResultSet statistics. */
    EmitRecordFunc EmitRecord;  /* Function pointer to Record reply routine. */
} ResultSet;

ResultSet* NewResultSet(RedisModuleCtx *ctx, bool distinct, bool compact);

void ResultSet_ReplyWithPreamble(ResultSet *set, AR_ExpNode **exps);

int ResultSet_AddRecord(ResultSet* set, Record r);

void ResultSet_Replay(ResultSet* set);

void ResultSet_Free(ResultSet* set);

#endif
