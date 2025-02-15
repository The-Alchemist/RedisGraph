#
# Copyright (c) 2015-2019 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Attribution Notice under the terms of the Apache License 2.0
#
# This work was created by the collective efforts of the openCypher community.
# Without limiting the terms of Section 6, any Derivative Work that is not
# approved by the public consensus process of the openCypher Implementers Group
# should not be described as “Cypher” (and Cypher® is a registered trademark of
# Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
# proposals for change that have been documented or implemented should only be
# described as "implementation extensions to Cypher" or as "proposed changes to
# Cypher that are not yet approved by the openCypher community".
#

#encoding: utf-8

Feature: MergeNodeAcceptance
  @skip
  Scenario: Merge node when no nodes exist
    Given an empty graph
    When executing query:
      """
      MERGE (a)
      RETURN count(*) AS n
      """
    Then the result should be:
      | n |
      | 1 |
    And the side effects should be:
      | +nodes | 1 |
@skip
  Scenario: Merge node with label
    Given an empty graph
    When executing query:
      """
      MERGE (a:Label)
      RETURN labels(a)
      """
    Then the result should be:
      | labels(a) |
      | ['Label'] |
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 1 |
@skip
  Scenario: Merge node with label add label on create
    Given an empty graph
    When executing query:
      """
      MERGE (a:Label)
        ON CREATE SET a:Foo
      RETURN labels(a)
      """
    Then the result should be:
      | labels(a)        |
      | ['Label', 'Foo'] |
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 2 |
@skip
  Scenario: Merge node with label add property on create
    Given an empty graph
    When executing query:
      """
      MERGE (a:Label)
        ON CREATE SET a.prop = 42
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | 42     |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Merge node with label when it exists
    Given an empty graph
    And having executed:
      """
      CREATE (:Label {id: 1})
      """
    When executing query:
      """
      MERGE (a:Label)
      RETURN a.id
      """
    Then the result should be:
      | a.id |
      | 1    |
    And no side effects
@skip
  Scenario: Merge node should create when it doesn't match, properties
    Given an empty graph
    And having executed:
      """
      CREATE ({prop: 42})
      """
    When executing query:
      """
      MERGE (a {prop: 43})
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | 43     |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
@skip
  Scenario: Merge node should create when it doesn't match, properties and label
    Given an empty graph
    And having executed:
      """
      CREATE (:Label {prop: 42})
      """
    When executing query:
      """
      MERGE (a:Label {prop: 43})
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | 43     |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
@skip
  Scenario: Merge node with prop and label
    Given an empty graph
    And having executed:
      """
      CREATE (:Label {prop: 42})
      """
    When executing query:
      """
      MERGE (a:Label {prop: 42})
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | 42     |
    And no side effects
@skip
  Scenario: Merge node with label add label on match when it exists
    Given an empty graph
    And having executed:
      """
      CREATE (:Label)
      """
    When executing query:
      """
      MERGE (a:Label)
        ON MATCH SET a:Foo
      RETURN labels(a)
      """
    Then the result should be:
      | labels(a)        |
      | ['Label', 'Foo'] |
    And the side effects should be:
      | +labels | 1 |
@skip
  Scenario: Merge node with label add property on update when it exists
    Given an empty graph
    And having executed:
      """
      CREATE (:Label)
      """
    When executing query:
      """
      MERGE (a:Label)
        ON CREATE SET a.prop = 42
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | null   |
    And no side effects
@skip
  Scenario: Merge node and set property on match
    Given an empty graph
    And having executed:
      """
      CREATE (:Label)
      """
    When executing query:
      """
      MERGE (a:Label)
        ON MATCH SET a.prop = 42
      RETURN a.prop
      """
    Then the result should be:
      | a.prop |
      | 42     |
    And the side effects should be:
      | +properties | 1 |
@skip
  Scenario: Should work when finding multiple elements
    Given an empty graph
    When executing query:
      """
      CREATE (:X)
      CREATE (:X)
      MERGE (:X)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 2 |
      | +labels | 1 |
@skip
  Scenario: Should handle argument properly
    Given an empty graph
    And having executed:
      """
      CREATE ({x: 42}),
        ({x: 'not42'})
      """
    When executing query:
      """
      WITH 42 AS x
      MERGE (c:N {x: x})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Should handle arguments properly with only write clauses
    Given an empty graph
    When executing query:
      """
      CREATE (a {p: 1})
      MERGE ({v: a.p})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 2 |
      | +properties | 2 |
@skip
  Scenario: Should be able to merge using property from match
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {name: 'A', bornIn: 'New York'})
      CREATE (:Person {name: 'B', bornIn: 'Ohio'})
      CREATE (:Person {name: 'C', bornIn: 'New Jersey'})
      CREATE (:Person {name: 'D', bornIn: 'New York'})
      CREATE (:Person {name: 'E', bornIn: 'Ohio'})
      CREATE (:Person {name: 'F', bornIn: 'New Jersey'})
      """
    When executing query:
      """
      MATCH (person:Person)
      MERGE (city:City {name: person.bornIn})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 3 |
      | +labels     | 1 |
      | +properties | 3 |
@skip
  Scenario: Should be able to use properties from match in ON CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {bornIn: 'New York'}),
        (:Person {bornIn: 'Ohio'})
      """
    When executing query:
      """
      MATCH (person:Person)
      MERGE (city:City)
        ON CREATE SET city.name = person.bornIn
      RETURN person.bornIn
      """
    Then the result should be:
      | person.bornIn |
      | 'New York'    |
      | 'Ohio'        |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Should be able to use properties from match in ON MATCH
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {bornIn: 'New York'}),
        (:Person {bornIn: 'Ohio'})
      """
    When executing query:
      """
      MATCH (person:Person)
      MERGE (city:City)
        ON MATCH SET city.name = person.bornIn
      RETURN person.bornIn
      """
    Then the result should be:
      | person.bornIn |
      | 'New York'    |
      | 'Ohio'        |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Should be able to use properties from match in ON MATCH and ON CREATE
    Given an empty graph
    And having executed:
      """
      CREATE (:Person {bornIn: 'New York'}),
        (:Person {bornIn: 'Ohio'})
      """
    When executing query:
        """
        MATCH (person:Person)
        MERGE (city:City)
          ON MATCH SET city.name = person.bornIn
          ON CREATE SET city.name = person.bornIn
        RETURN person.bornIn
        """
    Then the result should be:
      | person.bornIn |
      | 'New York'    |
      | 'Ohio'        |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Should be able to set labels on match
    Given an empty graph
    And having executed:
      """
      CREATE ()
      """
    When executing query:
      """
      MERGE (a)
        ON MATCH SET a:L
      """
    Then the result should be empty
    And the side effects should be:
      | +labels | 1 |
@skip
  Scenario: Should be able to set labels on match and on create
    Given an empty graph
    And having executed:
      """
      CREATE (), ()
      """
    When executing query:
      """
      MATCH ()
      MERGE (a:L)
        ON MATCH SET a:M1
        ON CREATE SET a:M2
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes  | 1 |
      | +labels | 3 |
@skip
  Scenario: Should support updates while merging
    Given an empty graph
    And having executed:
      """
      UNWIND [0, 1, 2] AS x
      UNWIND [0, 1, 2] AS y
      CREATE ({x: x, y: y})
      """
    When executing query:
      """
      MATCH (foo)
      WITH foo.x AS x, foo.y AS y
      MERGE (:N {x: x, y: y + 1})
      MERGE (:N {x: x, y: y})
      MERGE (:N {x: x + 1, y: y})
      RETURN x, y
      """
    Then the result should be:
      | x | y |
      | 0 | 0 |
      | 0 | 1 |
      | 0 | 2 |
      | 1 | 0 |
      | 1 | 1 |
      | 1 | 2 |
      | 2 | 0 |
      | 2 | 1 |
      | 2 | 2 |
    And the side effects should be:
      | +nodes      | 15 |
      | +labels     | 1  |
      | +properties | 30 |
@skip
  Scenario: Merge must properly handle multiple labels
    Given an empty graph
    And having executed:
      """
      CREATE (:L:A {prop: 42})
      """
    When executing query:
      """
      MERGE (test:L:B {prop: 42})
      RETURN labels(test) AS labels
      """
    Then the result should be:
      | labels     |
      | ['L', 'B'] |
    And the side effects should be:
      | +nodes      | 1 |
      | +labels     | 1 |
      | +properties | 1 |
@skip
  Scenario: Merge followed by multiple creates
    Given an empty graph
    When executing query:
      """
      MERGE (t:T {id: 42})
      CREATE (f:R)
      CREATE (t)-[:REL]->(f)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
      | +properties    | 1 |
@skip
  Scenario: Unwind combined with merge
    Given an empty graph
    When executing query:
      """
      UNWIND [1, 2, 3, 4] AS int
      MERGE (n {id: int})
      RETURN count(*)
      """
    Then the result should be:
      | count(*) |
      | 4        |
    And the side effects should be:
      | +nodes      | 4 |
      | +properties | 4 |
@skip
  Scenario: Merges should not be able to match on deleted nodes
    Given an empty graph
    And having executed:
      """
      CREATE (:A {value: 1}),
        (:A {value: 2})
      """
    When executing query:
      """
      MATCH (a:A)
      DELETE a
      MERGE (a2:A)
      RETURN a2.value
      """
    Then the result should be:
      | a2.value |
      | null     |
      | null     |
    And the side effects should be:
      | +nodes      | 1 |
      | -nodes      | 2 |
      | -properties | 2 |
@skip
  Scenario: ON CREATE on created nodes
    Given an empty graph
    When executing query:
      """
      MERGE (b)
        ON CREATE SET b.created = 1
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 1 |
      | +properties    | 1 |

