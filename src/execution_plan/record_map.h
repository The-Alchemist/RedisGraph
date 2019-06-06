/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#include "../util/triemap/triemap.h"
#include "../arithmetic/arithmetic_expression.h"
#include "../ast/ast.h"
#include <limits.h>

typedef struct {
    TrieMap *map;
    uint record_len;
} RecordMap;

typedef const void* AST_IDENTIFIER;
#define IDENTIFIER_NOT_FOUND UINT_MAX

uint RecordMap_GetRecordIDFromReference(RecordMap *map, AST_IDENTIFIER entity);
uint RecordMap_ReferenceToRecordID(RecordMap *map, AST_IDENTIFIER identifier);
uint RecordMap_ExpressionToRecordID(RecordMap *map, AR_ExpNode *exp);

uint RecordMap_LookupEntity(RecordMap *map, AST_IDENTIFIER entity);
uint RecordMap_LookupAlias(RecordMap *map, const char *alias);
uint RecordMap_LookupEntityID(RecordMap *record_map, uint id);

uint RecordMap_FindOrAddASTEntity(RecordMap *record_map, const AST *ast, const cypher_astnode_t *entity);
uint RecordMap_FindOrAddID(RecordMap *record_map, uint entity_id);

RecordMap *RecordMap_New(void);
void RecordMap_Free(RecordMap *record_map);
