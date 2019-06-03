/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>
#include "query_executor.h"
#include "util/arr.h"
#include "graph/graph.h"
#include "util/vector.h"
#include "schema/schema.h"
#include "procedures/procedure.h"
#include "arithmetic/repository.h"
#include "arithmetic/agg_ctx.h"
#include "arithmetic/repository.h"
#include "../deps/libcypher-parser/lib/src/cypher-parser.h"

/* Incase procedure call is missing its yield part
 * include procedure outputs. */
// TODO
// static void _inlineProcedureYield(AST_ProcedureCallNode *node) {
    // if(node->yield) return;

    // ProcedureCtx *proc = Proc_Get(node->procedure);
    // if(!proc) return;

    // unsigned int output_count = array_len(proc->output);

    // node->yield = array_new(char*, output_count);
    // for(int i = 0; i < output_count; i++) {
        // node->yield = array_append(node->yield, strdup(proc->output[i]->name));
    // }
// }

AST_Validation AST_PerformValidations(RedisModuleCtx *ctx, const AST *ast) {
    char *reason;
    AST_Validation res = AST_Validate(ast, &reason);
    if (res != AST_VALID) {
        RedisModule_ReplyWithError(ctx, reason);
        free(reason);
        return AST_INVALID;
    }
    return AST_VALID;
}

/* Counts the number of right to left edges,
 * if it's greater than half the number of edges in pattern
 * return true.*/
// static bool _AST_should_reverse_pattern(Vector *pattern) {
    // int transposed = 0; // Number of transposed edges
    // int edge_count = 0; // Total number of edges.
    // int pattern_length = Vector_Size(pattern);

    // // Count how many edges are going right to left.
    // for(int i = 0; i < pattern_length; i++) {
        // AST_GraphEntity *match_element;
        // Vector_Get(pattern, i, &match_element);

        // if(match_element->t != N_LINK) continue;

        // edge_count++;
        // AST_LinkEntity *edge = (AST_LinkEntity*)match_element;
        // if(edge->direction ==  N_RIGHT_TO_LEFT) transposed++;
    // }

    // // No edges.
    // if(edge_count == 0) return false;
    // return (transposed > edge_count/2);
// }

/* Construct a new MATCH clause by cloning the current one
 * and reversing traversal patterns to reduce matrix transpose
 * operation. */
// TODO re-implement this
// static void _AST_reverse_match_patterns(AST *ast) {
    // size_t pattern_count = Vector_Size(ast->matchNode->patterns);
    // Vector *patterns = NewVector(Vector*, pattern_count);

    // for(int i = 0; i < pattern_count; i++) {
        // Vector *pattern;
        // Vector_Get(ast->matchNode->patterns, i, &pattern);

        // size_t pattern_length = Vector_Size(pattern);
        // Vector *v = NewVector(AST_GraphEntity*, pattern_length);

        // if(!_AST_should_reverse_pattern(pattern)) {
            // // No need to reverse, simply clone pattern.
            // for(int j = 0; j < pattern_length; j++) {
                // AST_GraphEntity *e;
                // Vector_Get(pattern, j, &e);
                // e = Clone_AST_GraphEntity(e);
                // Vector_Push(v, e);
            // }
        // }
        // else {
            /* Reverse pattern:
             * Create a new pattern where edges been reversed.
             * Nodes should be introduced in reverse order:
             * (C)<-[B]-(A)
             * (A)-[B]->(C) */
            // for(int j = pattern_length-1; j >= 0; j--) {
                // AST_GraphEntity *e;
                // Vector_Get(pattern, j, &e);
                // e = Clone_AST_GraphEntity(e);

                // if(e->t == N_LINK) {
                    // AST_LinkEntity *l = (AST_LinkEntity*)e;
                    // // Reverse pattern.
                    // if(l->direction == N_RIGHT_TO_LEFT) l->direction = N_LEFT_TO_RIGHT;
                    // else l->direction = N_RIGHT_TO_LEFT;
                // }
                // Vector_Push(v, e);
            // }
        // }
        // Vector_Push(patterns, v);
    // }

    // // Free old MATCH clause.
    // Free_AST_MatchNode(ast->matchNode);
    // // Update AST MATCH clause.
    // ast->matchNode = New_AST_MatchNode(patterns);
// }

// static void _AST_optimize_traversal_direction(AST *ast) {
    /* Inspect each MATCH pattern,
     * see if the number of edges going from right to left ()<-[]-()
     * is greater than the number of edges going from left to right ()-[]->()
     * in which case it's worth reversing the pattern to reduce
     * matrix transpose operations. */

    // bool should_reverse = false;
    // size_t pattern_count = Vector_Size(ast->matchNode->patterns);
    // for(int i = 0; i < pattern_count; i++) {
        // Vector *pattern;
        // Vector_Get(ast->matchNode->patterns, i, &pattern);

        // if(_AST_should_reverse_pattern(pattern)) {
            // should_reverse = true;
            // break;
        // }
    // }

    // if(should_reverse) _AST_reverse_match_patterns(ast);
// }

AR_ExpNode** _ReturnExpandAll(AST *ast) {
    unsigned int identifier_count = array_len(ast->defined_entities);
    AR_ExpNode **return_expressions = array_new(AR_ExpNode*, identifier_count);

    for (unsigned int i = 0; i < identifier_count; i ++) {
        AR_ExpNode *entity = ast->defined_entities[i];
        const char *alias = entity->operand.variadic.entity_alias;
        if (alias) {
            return_expressions = array_append(return_expressions, entity);
        }
    }
    return return_expressions;
}

// Handle RETURN entities
AR_ExpNode** _BuildReturnExpressions(AST *ast, const cypher_astnode_t *ret_clause) {
    // Query is of type "RETURN *",
    // collect all defined identifiers and create return elements for them
    if (cypher_ast_return_has_include_existing(ret_clause)) return _ReturnExpandAll(ast);

    unsigned int count = cypher_ast_return_nprojections(ret_clause);
    AR_ExpNode **return_expressions = array_new(AR_ExpNode*, count);
    for (unsigned int i = 0; i < count; i++) {
        const cypher_astnode_t *projection = cypher_ast_return_get_projection(ret_clause, i);
        const cypher_astnode_t *expr = cypher_ast_projection_get_expression(projection);

        AR_ExpNode *exp = NULL;
        char *identifier = NULL;
        uint id = NOT_IN_RECORD;

        if (cypher_astnode_type(expr) == CYPHER_AST_IDENTIFIER) {
            // Retrieve "a" from "RETURN a" or "RETURN a AS e"
            identifier = (char*)cypher_ast_identifier_get_name(expr);
            id = AST_GetEntityFromAlias(ast, (char*)identifier);
        }

        if (id == NOT_IN_RECORD) {
            // Identifier did not appear in previous clauses.
            // It may be a constant or a function call (or other?)
            // Create a new entity to represent it.
            exp = AR_EXP_FromExpression(ast, expr);

            // Make space for entity in record
            unsigned int id = AST_MapEntity(ast, expr);
            AR_EXP_AssignRecordIndex(exp, id);
            // Add entity to the set of entities to be populated
            ast->defined_entities = array_append(ast->defined_entities, exp);
        }

        // If the projection is aliased, add the alias to mappings and Record
        const char *alias = NULL;
        const cypher_astnode_t *alias_node = cypher_ast_projection_get_alias(projection);
        if (alias_node) {
            // The projection either has an alias (AS) or is a function call.
            alias = cypher_ast_identifier_get_name(alias_node);
            AST_MapAlias(ast, alias);
            // exp->alias = alias;
            exp->operand.variadic.entity_alias_idx = id;
        } else {
            const cypher_astnode_t *expr = cypher_ast_projection_get_expression(projection);
            if (cypher_astnode_type(expr) == CYPHER_AST_IDENTIFIER) {
                // Retrieve "a" from "RETURN a" or "RETURN a AS e"
                identifier = (char*)cypher_ast_identifier_get_name(expr);
            }
            exp->operand.variadic.entity_alias = identifier;
            exp->operand.variadic.entity_alias_idx = id;
        }
        return_expressions = array_append(return_expressions, exp);
    }

    return return_expressions;
}

AR_ExpNode** AST_BuildOrderExpressions(AST *ast, const cypher_astnode_t *order_clause) {
    // Handle ORDER entities
    assert(order_clause);

    bool ascending = true;
    unsigned int nitems = cypher_ast_order_by_nitems(order_clause);
    AR_ExpNode **order_exps = array_new(AR_ExpNode*, nitems);

    for (unsigned int i = 0; i < nitems; i ++) {
        const cypher_astnode_t *item = cypher_ast_order_by_get_item(order_clause, i);
        const cypher_astnode_t *cypher_exp = cypher_ast_sort_item_get_expression(item);
        AR_ExpNode *exp;
        if (cypher_astnode_type(cypher_exp) == CYPHER_AST_IDENTIFIER) {
            // Reference to an alias in the query - associate with existing AR_ExpNode
            const char *alias = cypher_ast_identifier_get_name(cypher_exp);
            // Clone the expression so that we can free safely
            exp = AR_EXP_Clone(AST_GetEntityFromAlias(ast, alias));
            /* TODO There is a bit of oddness here, in that we should be able to create
             * a reference node (as commented), but if the aliased entity is an aggregate,
             * it will not register properly in op_aggregate's _classify_expressions */
            // AR_ExpNode *referred_exp = AST_GetEntityFromAlias(ast, (char*)alias);
            // exp = AR_EXP_NewReferenceNode((char*)alias, referred_exp->record_idx, false);
        } else {
            // Independent operator like:
            // ORDER BY COUNT(a)
            exp = AR_EXP_FromExpression(ast, cypher_exp);
        }

        order_exps = array_append(order_exps, exp);
        // TODO direction should be specifiable per order entity
        ascending = cypher_ast_sort_item_is_ascending(item);
    }

    // *direction = ascending ? DIR_ASC : DIR_DESC;

    return order_exps;
}

AR_ExpNode** AST_BuildReturnExpressions(AST *ast, const cypher_astnode_t *ret_clause) {
    AR_ExpNode **exps = _BuildReturnExpressions(ast, ret_clause);

    return exps;
}

// Handle WITH entities
AR_ExpNode** AST_BuildWithExpressions(AST *ast, const cypher_astnode_t *with_clause) {

    // TODO is this a thing?
    // Query is of type "with *",
    // collect all defined identifiers and create with elements for them
    // if (cypher_ast_with_has_include_existing(with_clause)) return _ReturnExpandAll(ast);

    unsigned int count = cypher_ast_with_nprojections(with_clause);
    AR_ExpNode **with_expressions = array_new(AR_ExpNode*, count);
    for (unsigned int i = 0; i < count; i++) {
        const cypher_astnode_t *projection = cypher_ast_with_get_projection(with_clause, i);
        const cypher_astnode_t *expr = cypher_ast_projection_get_expression(projection);

        uint record_id = NOT_IN_RECORD;
        const char *identifier = NULL;

        if (cypher_astnode_type(expr) == CYPHER_AST_IDENTIFIER) {
            // Retrieve "a" from "with a" or "with a AS e"
            identifier = cypher_ast_identifier_get_name(expr);
            record_id = AST_GetEntityFromAlias(ast, (char*)identifier);
        }

        AR_ExpNode *exp = NULL;
        if (record_id == NOT_IN_RECORD) {
            // Identifier did not appear in previous clauses.
            // It may be a constant or a function call (or other?)
            // Create a new entity to represent it.
            exp = AR_EXP_FromExpression(ast, expr);

            // Make space for entity in record
            record_id = AST_MapAlias(ast, identifier);
        }

        // If the projection is aliased, add the alias to mappings and Record
        char *alias = NULL;
        const cypher_astnode_t *alias_node = cypher_ast_projection_get_alias(projection);
        if (alias_node) {
            // The projection either has an alias (AS) or is a function call.
            alias = (char*)cypher_ast_identifier_get_name(alias_node);

            // Associate alias with the expression
            AST_AssociateAliasWithID(ast, alias, record_id);
            // exp->alias = alias;
            with_expressions = array_append(with_expressions, exp);
        } else {
            // exp->alias = identifier;
            with_expressions = array_append(with_expressions, exp);
        }
    }

    return with_expressions;
}

