import redis
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from disposableredis import DisposableRedis

from redisgraph import Graph, Node, Edge

r = None
graph_name = "G"
redis_graph = None

def redis():
    graph_so = os.path.dirname(os.path.abspath(__file__)) + '/../../../src/redisgraph.so'
    print graph_so
    return DisposableRedis(loadmodule=graph_so)

def _brand_new_redis():
    global r
    if r is not None:
        r.stop()

    r = redis()
    r.start()
    return r.client()

    # return redis.Redis()

def empty_graph():
    global redis_graph
    
    redis_con = _brand_new_redis()
    redis_graph = Graph("G", redis_con)

    # Create a graph with a single node.
    redis_graph.add_node(Node())
    redis_graph.commit()

    # Delete node to have an empty graph.
    redis_graph.query("MATCH (n) DELETE n")

def any_graph():
    return empty_graph()

def binary_tree_graph1():
    global redis_graph

    redis_con = _brand_new_redis()
    redis_graph = Graph("G1", redis_con)
    redis_graph.query("CREATE(a: A {name: 'a'}),    \
                      (b1: X {name: 'b1'}),         \
                      (b2: X {name: 'b2'}),         \
                      (b3: X {name: 'b3'}),         \
                      (b4: X {name: 'b4'}),         \
                      (c11: X {name: 'c11'}),       \
                      (c12: X {name: 'c12'}),       \
                      (c21: X {name: 'c21'}),       \
                      (c22: X {name: 'c22'}),       \
                      (c31: X {name: 'c31'}),       \
                      (c32: X {name: 'c32'}),       \
                      (c41: X {name: 'c41'}),       \
                      (c42: X {name: 'c42'})        \
                      CREATE(a)-[:KNOWS] -> (b1),   \
                      (a)-[:KNOWS] -> (b2),         \
                      (a)-[:FOLLOWS] -> (b3),       \
                      (a)-[:FOLLOWS] -> (b4)        \
                      CREATE(b1)-[:FRIEND] -> (c11),\
                      (b1)-[:FRIEND] -> (c12),      \
                      (b2)-[:FRIEND] -> (c21),      \
                      (b2)-[:FRIEND] -> (c22),      \
                      (b3)-[:FRIEND] -> (c31),      \
                      (b3)-[:FRIEND] -> (c32),      \
                      (b4)-[:FRIEND] -> (c41),      \
                      (b4)-[:FRIEND] -> (c42)       \
                      CREATE(b1)-[:FRIEND] -> (b2), \
                      (b2)-[:FRIEND] -> (b3),       \
                      (b3)-[:FRIEND] -> (b4),       \
                      (b4)-[:FRIEND] -> (b1)        \
                      ")


def binary_tree_graph2():
    global redis_graph

    redis_con = _brand_new_redis()
    redis_graph = Graph("G2", redis_con)
    redis_graph.query("CREATE(a: A {name: 'a'}),    \
                      (b1: X {name: 'b1'}),         \
                      (b2: X {name: 'b2'}),         \
                      (b3: X {name: 'b3'}),         \
                      (b4: X {name: 'b4'}),         \
                      (c11: X {name: 'c11'}),       \
                      (c12: Y {name: 'c12'}),       \
                      (c21: X {name: 'c21'}),       \
                      (c22: Y {name: 'c22'}),       \
                      (c31: X {name: 'c31'}),       \
                      (c32: Y {name: 'c32'}),       \
                      (c41: X {name: 'c41'}),       \
                      (c42: Y {name: 'c42'})        \
                      CREATE(a)-[:KNOWS] -> (b1),   \
                      (a)-[:KNOWS] -> (b2),         \
                      (a)-[:FOLLOWS] -> (b3),       \
                      (a)-[:FOLLOWS] -> (b4)        \
                      CREATE(b1)-[:FRIEND] -> (c11),\
                      (b1)-[:FRIEND] -> (c12),      \
                      (b2)-[:FRIEND] -> (c21),      \
                      (b2)-[:FRIEND] -> (c22),      \
                      (b3)-[:FRIEND] -> (c31),      \
                      (b3)-[:FRIEND] -> (c32),      \
                      (b4)-[:FRIEND] -> (c41),      \
                      (b4)-[:FRIEND] -> (c42)       \
                      CREATE(b1)-[:FRIEND] -> (b2), \
                      (b2)-[:FRIEND] -> (b3),       \
                      (b3)-[:FRIEND] -> (b4),       \
                      (b4)-[:FRIEND] -> (b1)        \
                      ")

def query(q):
    return redis_graph.query(q)

def teardown():
    global r
    if r is not None:
        r.stop()
