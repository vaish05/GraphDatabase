#!/usr/bin/env python3

"""
Neo4J.py

Whoever was coding this, had Java or so in mind. In Python one does not end code lines with a semicolon. :-)
(that was me - Raakesh Premkumar! ;-))

"""


from GraphRepr import *
import pickle
import math


'''
    Utility / Helper functions
'''


def _neo4j_strize(value):
    '''
        strize input value according to Cipher standard

        if value is string, surround it with single quote,
            otherwise print to str as it.
    '''
    if isinstance(value, str):
        return "'" + value + "'"
    return str(value)


def _classes2str(obj):
    '''
        convert class labels of a node into query string
    '''
    for key, value in obj._attrs.items():
        temp = [None]
        if isinstance(value, float):
            obj._attrs[key] = 'NaN'
        if value == temp:
            obj._attrs[key] = 'None'
        if value == float('nan'):
            obj._attrs[key] = 'NaN'

    if "label" in obj._attrs:
        temp = obj._attrs["label"]
        temp = "_".join(temp.split())
        return ':None' if obj._attrs["label"] is None else ':' + temp
    elif "Label" in obj._attrs:
        temp = obj._attrs["Label"]
        temp = "_".join(temp.split())
        return ':None' if obj._attrs["Label"] is None else ':' + temp
    else:
        return ':None'

def _classes2stredge(obj):
    '''
        convert class labels of a node into query string
    '''
    for key, value in obj._attrs.items():
        temp = [None]
        if isinstance(value, float):
            obj._attrs[key] = 'NaN'
        if value == temp:
            obj._attrs[key] = 'None'
        if value == float('nan'):
            obj._attrs[key] = 'NaN'

    if "name" in obj._attrs:
        temp = obj._attrs["name"]
        temp = "_".join(temp.split())
        return ':None' if obj._attrs["name"] is None else ':' + temp
    elif "Name" in obj._attrs:
        temp = obj._attrs["Name"]
        temp = "_".join(temp.split())
        return ':None' if obj._attrs["Name"] is None else ':' + temp
    else:
        return ':None'

def isIntransitiveVerbTerminal(obj):
    '''
	check if a node is IntransitiveVerbTerminal
    '''
    
    temp = ""
    if "name" in obj._attrs:
        temp = obj._attrs["name"]
    elif "Name" in obj._attrs:
        temp = obj._attrs["Name"]

    if temp == "IntransitiveVerbTerminal":
        return True
    return False

def _attrs2str(obj):
    '''
        convert attributes of a node into query string
    '''
    if obj._attrs is None:
        return ''
    output = [
                 key + ':' + _neo4j_strize(value)
                 for key, value in obj._attrs.items()
             ]
    return '{' + ','.join(output) + '}'


'''
    Mapper functions and specific helpers for node objects
        (map node objects to Cipher queries)
'''


def _node_attr_equality_string(node, key, value, base):
    return '%s.%s = %s' % (base, key, _neo4j_strize(value))


def _node_db_repr(node):
    return '(a%s %s)' % (_classes2str(node), _attrs2str(node))


def node_create_query(node):
    '''
        Mapper: from node to CREATE query in Cypher
            assume node does not exist in db
			MERGE checks if a node exists in the db.
			If not, creates it.
    '''
    return 'MERGE %s' % _node_db_repr(node)


'''
    Mapper functions and specfic helpers for edge objects
        (map edge objects to Cipher queries)
'''


def _edge_node1_attr_check_str(edge):
    return ' AND '.join([
        _node_attr_equality_string(edge._node1, k, v, 'a')
        for k, v in edge._node1._attrs.items()
    ])


def _edge_node2_attr_check_str(edge):
    return ' AND '.join([
        _node_attr_equality_string(edge._node2, k, v, 'b')
        for k, v in edge._node2._attrs.items()
    ])


def _edge_ends_attr_check_str(edge):
    return _edge_node1_attr_check_str(edge) + ' AND ' + \
           _edge_node2_attr_check_str(edge)


def edge_create_query(edge, which_node):
    '''
        Mapper: from edge to CREATE query in Cipher
            assume nodes in both end already exist in db
    '''

    if which_node == "node1":
        return 'MATCH (b%s), (b%s) WHERE %s MERGE (b)-[e%s]->(b)' % (
        	_classes2str(edge._node2), _classes2str(edge._node2),
        	_edge_node2_attr_check_str(edge) + ' AND ' + _edge_node2_attr_check_str(edge),
        	_classes2stredge(edge) + ' ' + _attrs2str(edge),
    	)
    elif which_node == "node2":
        #print("in here");
        return 'MATCH (a%s), (a%s) WHERE %s MERGE (a)-[e%s]->(a)' % (
                _classes2str(edge._node1), _classes2str(edge._node1),
                _edge_node1_attr_check_str(edge) + ' AND ' + _edge_node1_attr_check_str(edge),
                _classes2stredge(edge) + ' ' + _attrs2str(edge),
        )
    else: 
        return 'MATCH (a%s), (b%s) WHERE %s MERGE (a)-[e%s]->(b)' % (
                _classes2str(edge._node1), _classes2str(edge._node2),
                _edge_ends_attr_check_str(edge),
                _classes2stredge(edge) + ' ' + _attrs2str(edge),
        )


def node_test():
    '''
        small hand-craft test for the class of Neo4J node
    '''
    node1 = Node({'Person': None}, {'name': 'Ken', 'title': 'Developer'})
    node2 = Node({'Person': None}, {'name': 'Alice', 'title': 'Student'})
    print(node1)
    print(node2)
    print(node_create_query(node1))
    print(node_create_query(node2))


def createCypherQuery():
    '''
        small hand-craft test for the class of Neo4J edge
    '''
    node1 = Node({'Person': None}, {'name': 'Ken', 'title': 'Developer'})
    node2 = Node({'Person': None}, {'name': 'Alice', 'title': 'Student'})
    edge = Edge(node1, node2, None, None,
                {'degree': 'extremely'}, {'social': None})
    print(edge_create_query(edge, "None"))
    return edge_create_query(edge, "None")


def createCypherQueries(edgeList):

    """
    This is the function which will be invoked from the SemDiscUtils.py

    Accepts edgeList as an argument and returns a bunch of cypher queries to the SemDiscUtils.py

    return : String of Cypher Queries
    """

    queryBreaker = "WITH 1 as dummy"
    queries = ""
    counter = len(edgeList)
    for edge in edgeList:
        #isTerminal = isIntransitiveVerbTerminal(edge._node1)
        isTerminal = False
        if isTerminal:
                #print("node1");
                queries += "\n".join( (node_create_query(edge._node2),
                                     queryBreaker,
                                     (edge_create_query(edge,"node1")), "\n"))
        elif isTerminal:
                #print("node2");
                queries += "\n".join( (node_create_query(edge._node1),
                                     queryBreaker,
                                     (edge_create_query(edge, "node2")), "\n"))
        else:
                queries += "\n".join( (node_create_query(edge._node1),
                                     queryBreaker,
                                     node_create_query(edge._node2),
                                     queryBreaker,
                                     (edge_create_query(edge, "none")), "\n"))
        counter -= 1
        if counter > 0:
            queries += "".join( (queryBreaker, "\n") )
            #print(queries)
    return queries


def readPickleTest():
    """
    TODO you should not call pickle directly, rather load the file into memory and submit the string to our
    function as defined in SemDiscUtils.py

    This is for test purposes only. The createCypherQueries(edgeList) will be called from SemDiscUtils.py
    which in turn returns a string of cypher queries

    :return:
    """
    edgeList = pickle.load(open("edgeList.pkl", "rb"))
    #print(edgeList);
    createCypherQueries(edgeList)


if __name__ == '__main__':
    node_test()
    print('')
    createCypherQuery()
    readPickleTest()
