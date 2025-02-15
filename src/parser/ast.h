/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef AST_H
#define AST_H

#include <stdbool.h>
#include "../value.h"
#include "./ast_common.h"
#include "../redismodule.h"
#include "../util/vector.h"
#include "./clauses/clauses.h"

typedef enum {
	AST_VALID,
	AST_INVALID
} AST_Validation;

typedef struct {
	AST_MatchNode *matchNode;
	AST_CreateNode *createNode;
	AST_MergeNode *mergeNode;
	AST_SetNode *setNode;
	AST_DeleteNode *deleteNode;
	AST_WhereNode *whereNode;
	AST_ReturnNode *returnNode;
	AST_OrderNode *orderNode;
	AST_LimitNode *limitNode;
	AST_SkipNode *skipNode;
	AST_IndexNode *indexNode;
	AST_UnwindNode *unwindNode;
	AST_WithNode *withNode;
	AST_ProcedureCallNode *callNode;
	TrieMap *_aliasIDMapping;	// Mapping between aliases and IDs.
} AST;

AST* AST_New(AST_MatchNode *matchNode, AST_WhereNode *whereNode,
						 AST_CreateNode *createNode, AST_MergeNode *mergeNode,
						 AST_SetNode *setNode, AST_DeleteNode *deleteNode,
						 AST_ReturnNode *returnNode, AST_OrderNode *orderNode,
						 AST_SkipNode *skipNode, AST_LimitNode *limitNode,
						 AST_IndexNode *indexNode, AST_UnwindNode *unwindNode,
						 AST_ProcedureCallNode *callNode);

// AST clause validations.
AST_Validation AST_Validate(const AST* ast, char **reason);

// Returns number of aliases defined in AST.
int AST_AliasCount(const AST *ast);

// Returns alias ID.
int AST_GetAliasID(const AST *ast, char *alias);

void AST_NameAnonymousNodes(AST *ast);

void AST_MapAliasToID(AST *ast, AST_WithNode *prevWithClause);

// Returns a triemap of all identifiers defined by ast.
TrieMap* AST_Identifiers(const AST *ast);

// Returns a triemap of AST_GraphEntity references from clauses that can specify nodes or edges.
TrieMap* AST_CollectEntityReferences(AST **ast);

/* Returns true if given ast describes projection 
 * i.e. contains RETURN, WITH clauses. */
bool AST_Projects(const AST *ast);

/* Returns true if AST is empty. */
bool AST_Empty(const AST *ast);

// Checks if AST represent a read only query.
bool AST_ReadOnly(AST **ast);

void AST_Free(AST **ast);

#endif
