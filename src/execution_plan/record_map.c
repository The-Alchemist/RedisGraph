/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "record_map.h"
#include "../util/rmalloc.h"

uint* _BuildMapValue(uint id) {
    // TODO so many unnecessary allocs
    uint *id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    return id_ptr;
}

uint RecordMap_GetRecordIDFromReference(RecordMap *record_map, AST_IDENTIFIER entity) {
    uint *id = TrieMap_Find(record_map->map, (char*)&entity, sizeof(entity));
    if (id == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;
    return *id;
}

uint RecordMap_ReferenceToRecordID(RecordMap *record_map, AST_IDENTIFIER identifier) {
    uint *id_ptr = TrieMap_Find(record_map->map, (char*)&identifier, sizeof(identifier));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    uint id = record_map->record_len++;
    id_ptr = _BuildMapValue(id);
    TrieMap_Add(record_map->map, (char*)&identifier, sizeof(identifier), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

uint RecordMap_ExpressionToRecordID(RecordMap *record_map, AR_ExpNode *exp) {
    uint *id_ptr = TrieMap_Find(record_map->map, (char*)&exp, sizeof(AR_ExpNode));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    uint id = IDENTIFIER_NOT_FOUND;
    // If the expression contains an alias, map it first, and re-use its Record ID if one is already assigned
    if (exp->type == AR_EXP_OPERAND && exp->operand.type == AR_EXP_VARIADIC && exp->operand.variadic.entity_alias) {
        id = RecordMap_LookupAlias(record_map, exp->operand.variadic.entity_alias);
    }

    if (id == IDENTIFIER_NOT_FOUND) id = record_map->map->cardinality;

    id_ptr = rm_malloc(sizeof(uint));
    *id_ptr = id;
    TrieMap_Add(record_map->map, (char*)&exp, sizeof(exp), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return *id_ptr;
}

uint RecordMap_LookupEntity(RecordMap *record_map, AST_IDENTIFIER entity) {
    uint *id = TrieMap_Find(record_map->map, (char*)&entity, sizeof(entity));
    if (id == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;
    return *id;
}

uint RecordMap_LookupAlias(RecordMap *record_map, const char *alias) {
    uint *id_ptr = TrieMap_Find(record_map->map, (char*)alias, strlen(alias));
    if (id_ptr == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;

    return *id_ptr;
}

uint RecordMap_LookupEntityID(RecordMap *record_map, uint id) {
    uint *id_ptr = TrieMap_Find(record_map->map, (char*)&id, sizeof(id));
    if (id_ptr == TRIEMAP_NOTFOUND) return IDENTIFIER_NOT_FOUND;

    return *id_ptr;
}

uint RecordMap_FindOrAddASTEntity(RecordMap *record_map, const AST *ast, const cypher_astnode_t *entity) {
    // Ensure this is a new entity
    // assert(TrieMap_Find(record_map->map, (char*)&entity, sizeof(entity)) == TRIEMAP_NOTFOUND);

    uint *id_ptr = TrieMap_Find(record_map->map, (char*)&entity, sizeof(entity));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    uint id = record_map->record_len++;

    // Map AST ID
    uint ast_id = AST_GetEntityIDFromReference(ast, entity);
    id_ptr = _BuildMapValue(id);
    TrieMap_Add(record_map->map, (char*)&ast_id, sizeof(ast_id), id_ptr, TrieMap_DONT_CARE_REPLACE);

    // Map AST pointer
    id_ptr = _BuildMapValue(id);
    TrieMap_Add(record_map->map, (char*)&entity, sizeof(entity), id_ptr, TrieMap_DONT_CARE_REPLACE);

    // Map alias?

    // Map AR_ExpNode?

    return id;
}

uint RecordMap_FindOrAddID(RecordMap *record_map, uint entity_id) {
    // Ensure this is a new entity
    // assert(TrieMap_Find(record_map->map, (char*)&entity, sizeof(entity)) == TRIEMAP_NOTFOUND);

    uint *id_ptr = TrieMap_Find(record_map->map, (char*)&entity_id, sizeof(entity_id));
    if (id_ptr != TRIEMAP_NOTFOUND) return *id_ptr;

    uint id = record_map->record_len++;

    // Map ID value
    id_ptr = _BuildMapValue(id);
    TrieMap_Add(record_map->map, (char*)&entity_id, sizeof(entity_id), id_ptr, TrieMap_DONT_CARE_REPLACE);

    return id;
}

RecordMap *RecordMap_New() {
    RecordMap *record_map = rm_malloc(sizeof(RecordMap));
    record_map->map = NewTrieMap();
    record_map->record_len = 0;

    return record_map;
}

void RecordMap_Free(RecordMap *record_map) {
    TrieMap_Free(record_map->map, NULL);
    rm_free(record_map);
}

