#!/usr/bin/env python3

"""
StarDog.py

Authors: Vaishnavi Shrinivasan, Harsha Phulwani

"""

import pickle

'''
    Utility / Helper functions
'''

from rdflib.plugins.stores import sparqlstore
from SPARQLWrapper import JSON

def _stardog_strize(value):
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
    #Changed the type from NAN to Thing as in OWL all objects are derived from a "Thing" type
    for key, value in obj.items():
        temp = [None]
        if isinstance(value, float):
            obj[key] = 'Thing'
        if value == temp:
            obj[key] = 'Thing'
        if value == float('nan'):
            obj[key] = 'Thing'

    if "name" in obj:
        temp = obj["name"]
        temp = "_".join(temp.split())
        return ':None' if obj["name"] is None else temp
    elif "Name" in obj:
        temp = obj["Name"]
        temp = "_".join(temp.split())
        return ':None' if obj["Name"] is None else temp
    else:
        return ':None'


def _classes2str_edge(obj):
    '''
        convert class labels of a node into query string
    '''
    for key, value in obj.items():
        temp = [None]
        if isinstance(value, float):
            obj[key] = 'Thing'
        if value == temp:
            obj[key] = 'Thing'
        if value == float('nan'):
            obj[key] = 'Thing'

    if "name" in obj:
        temp = obj["name"]
        temp = "_".join(temp.split())
        return ':None' if obj["name"] is None else temp
    elif "Name" in obj:
        temp = obj["Name"]
        temp = "_".join(temp.split())
        return ':None' if obj["Name"] is None else temp
    else:
        return ':None'


def _attrs2str(obj):
    '''
        convert attributes of a node into query string
    '''
    output = ''
    if obj is None:
        return ''
    for key, value in obj.items():
        if key.lower() == 'type':
            output +=  "rdf:type bookURI:"+ value + ";"
            continue
        if (isinstance(value, list)):
            for element in value:
                output += 'bookURI:' + key + " " + _stardog_strize(element) + ";"
        else:
            output += 'bookURI:' + key + " " + _stardog_strize(value) + ";"
    result = output[0:len(output)-1] + "."
    return result



def _attrs2str_edge(obj):
    '''
        convert attributes of a node into query string
    '''
    output = ''
    if obj is None:
        return ''
    for key, value in obj.items():
        if (isinstance(value, list)):
            for element in value:
                output += 'bookURI:' + key + " " + _stardog_strize(element) + ";"
        else:
            output += 'bookURI:' + key + " " + _stardog_strize(value) + ";"
    retVal = output[:len(output) - 1] + '.'
    return retVal

def _attrs2str_node(obj):
    '''
        convert attributes of a node into query string
    '''
    output = ''
    if obj is None:
        return ''
    for key, value in obj.items():
        if (key.lower() == 'name'):
            output += 'bookURI:Edge' + key + " " + _stardog_strize(value) + "."
            break
    return output


'''
    Mapper functions and specific helpers for node objects
        (map node objects to Cipher queries)
'''


def _node_db_repr_edge(node):
    return _attrs2str_edge(node)


def _node_db_repr(node):
    return _attrs2str(node)


def node_create_query(node):
    """
    Mapper: from node to CREATE query in Cypher
    assume node does not exist in db
    MERGE checks if a node exists in the db.
    If not, creates it.
    """
    uriHashValue = _classes2str(node)
    uri = "<http://linguistic.technology/stardog/books4.owl#" + uriHashValue + ">\n"
    return uri + _node_db_repr(node)

def edge_create_query(node1, node2, edge):
    """
        Mapper: from edge to CREATE query in Cipher
            assume nodes in both end already exist in db
    """

    uriHashValuen1 = _classes2str_edge(node1)
    uriHashValuen2 = _classes2str_edge(node2)
    urin1 = "<http://linguistic.technology/stardog/books4.owl#" + uriHashValuen1 + ">\n"
    urin2 = "<http://linguistic.technology/stardog/books4.owl#" + uriHashValuen2 + ">\n"
    for key, value in edge.items():
        if key.lower() == 'name':
            return urin1 + ' bookURI:' + value + ' ' + urin2 + "."


def createSPARQLQueries(edgeList):
    queryPrefix = "PREFIX bookURI:  <http://linguistic.technology/stardog/books4.owl#>"
    queryBegin = "INSERT DATA {\n"
    queryEnd = "}"
    queries = ""
    for edge in edgeList:
        queries += "\n".join((node_create_query(edge['_node1']),
            node_create_query(edge['_node2']),
            (edge_create_query(edge['_node1'],edge['_node2'],edge['_attrs'])), "\n"))

    result = queryPrefix + "\n" + queryBegin + queries + queryEnd
    return result


def readPickleTest():
    """
    TODO you should not call pickle directly, rather load the file into memory and submit the string to our
    function as defined in SemDiscUtils.py

    This is for test purposes only. The createCypherQueries(edgeList) will be called from SemDiscUtils.py
    which in turn returns a string of cypher queries

    :return:
    """
    edgeList = [{'_node1': {'Name': 'Belle', 'Email_Id': 'belle@iu.edu', 'Type': 'Reader'},
                 '_node2': {'Name': 'Batman Hush', 'Type': 'Fiction', 'Year': 2006},
                 '_attrs': {'Name': 'reads1'}},
                {'_node1': {'Name': 'Raghuram Rajan', 'type': 'Author', 'Email_Id': 'rrajan@rbi.com', 'Phone': 11223345},
                 '_node2': {'Name': 'I Do What I Do', 'Type': 'Non-Fiction', 'ISBN': 9789352770144, 'Year': 2017},
                 '_attrs': {'Name': 'writes'}}]
    return edgeList


if __name__ == '__main__':
    edgeList = readPickleTest()
    finalQuery = createSPARQLQueries(edgeList)

    # Define the Stardog store
    endpointUpdate = 'http://linguistic.technology:5820/books_db1/update'
    store = sparqlstore.SPARQLUpdateStore()
    store.open((endpointUpdate, endpointUpdate))
    store.setCredentials('admin','admin')
    store.update(finalQuery)
    print("Inserted into stardog database")
