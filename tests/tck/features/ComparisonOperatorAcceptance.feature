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

Feature: ComparisonOperatorAcceptance
  @skip
  Scenario: Handling numerical ranges 1
    Given an empty graph
    And having executed:
      """
      UNWIND [1, 2, 3] AS i
      CREATE ({value: i})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 1 < n.value < 3
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 2       |
    And no side effects
  @skip
  Scenario: Handling numerical ranges 2
    Given an empty graph
    And having executed:
      """
      UNWIND [1, 2, 3] AS i
      CREATE ({value: i})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 1 < n.value <= 3
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 2       |
      | 3       |
    And no side effects
  @skip
  Scenario: Handling numerical ranges 3
    Given an empty graph
    And having executed:
      """
      UNWIND [1, 2, 3] AS i
      CREATE ({value: i})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 1 <= n.value < 3
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 1       |
      | 2       |
    And no side effects
  @skip
  Scenario: Handling numerical ranges 4
    Given an empty graph
    And having executed:
      """
      UNWIND [1, 2, 3] AS i
      CREATE ({value: i})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 1 <= n.value <= 3
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 1       |
      | 2       |
      | 3       |
    And no side effects
  @skip
  Scenario: Handling string ranges 1
    Given an empty graph
    And having executed:
      """
      UNWIND ['a', 'b', 'c'] AS c
      CREATE ({value: c})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 'a' < n.value < 'c'
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 'b'     |
    And no side effects
  @skip
  Scenario: Handling string ranges 2
    Given an empty graph
    And having executed:
      """
      UNWIND ['a', 'b', 'c'] AS c
      CREATE ({value: c})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 'a' < n.value <= 'c'
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 'b'     |
      | 'c'     |
    And no side effects
  @skip
  Scenario: Handling string ranges 3
    Given an empty graph
    And having executed:
      """
      UNWIND ['a', 'b', 'c'] AS c
      CREATE ({value: c})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 'a' <= n.value < 'c'
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 'a'     |
      | 'b'     |
    And no side effects
  @skip
  Scenario: Handling string ranges 4
    Given an empty graph
    And having executed:
      """
      UNWIND ['a', 'b', 'c'] AS c
      CREATE ({value: c})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 'a' <= n.value <= 'c'
      RETURN n.value
      """
    Then the result should be:
      | n.value |
      | 'a'     |
      | 'b'     |
      | 'c'     |
    And no side effects
  @skip
  Scenario: Handling empty range
    Given an empty graph
    And having executed:
      """
      CREATE ({value: 3})
      """
    When executing query:
      """
      MATCH (n)
      WHERE 10 < n.value <= 3
      RETURN n.value
      """
    Then the result should be:
      | n.value |
    And no side effects
  @skip
  Scenario: Handling long chains of operators
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {prop1: 3, prop2: 4})
      CREATE (b:B {prop1: 4, prop2: 5})
      CREATE (c:C {prop1: 4, prop2: 4})
      CREATE (a)-[:R]->(b)
      CREATE (b)-[:R]->(c)
      CREATE (c)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (n)-->(m)
      WHERE n.prop1 < m.prop1 = n.prop2 <> m.prop2
      RETURN labels(m)
      """
    Then the result should be:
      | labels(m) |
      | ['B']     |
    And no side effects
