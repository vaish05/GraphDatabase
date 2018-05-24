#!/usr/bin/env python3

"""
StarDogQA.py

Authors: Vaishnavi Shrinivasan, Harsha Phulwani

"""

import pickle

'''
    Utility / Helper functions
'''

from rdflib.plugins.stores import sparqlstore
from SPARQLWrapper import JSON

def createSPARQLQueries(edge):
    queryPrefix = "PREFIX bookURI:  <http://linguistic.technology/stardog/books4.owl#>"
    query = ""
    queryEnd = "}"
    if('_node1' in edge and '_node2' in edge and '_attrs' in edge):
        '''Query should be in this syntax
        PREFIX bookURI:  <http://linguistic.technology/stardog/books4.owl#>
        SELECT ?Email_Id WHERE
        {
        ?Thing bookURI:reads1 bookURI:I_Do_What_I_Do .
        ?Thing bookURI:Email_Id ?Email_Id 
        }
        '''

        query = "SELECT ?" +edge['_node2']['Name'] \
        + " WHERE { ?Thing bookURI:" + edge['_attrs']['Name'] \
        + " bookURI:" + (edge['_node1']['Name']).replace(' ','_') +" . ?Thing bookURI:" \
        + edge['_node2']['Name'] + "?" + edge['_node2']['Name']

    elif('_node1' in edge and '_node2' in edge and '_attr' not in edge):
        '''Query should be in this syntax
        PREFIX bookURI: < http: // linguistic.technology / stardog / books4.owl  # >
        SELECT ?Email_Id WHERE
        {
        ?Thing ?x "Raghuram Rajan".
        ?Thing bookURI: Email_Id ?Email_Id
        }'''

        query = "SELECT ?" + edge['_node2']['Name'] \
                + " WHERE { ?Thing ?x \"" \
                + edge['_node1']['Name'] + "\" . ?Thing bookURI:" \
                + edge['_node2']['Name'] + "?" + edge['_node2']['Name']
    elif('_node1' in edge and '_node2' not in edge and '_attr' not in edge):
        '''Query should be in this syntax
        PREFIX bookURI:  <http://linguistic.technology/stardog/books4.owl#>
        SELECT ?Thing WHERE
        {
        ?Thing ?x "Harsha Phulwani" .
        } '''

        query = "SELECT ?Thing WHERE { ?Thing ?x\"" + edge['_node1']['Name'] +"\" ."
    else:
        pass

    result = queryPrefix + "\n" + query + queryEnd
    return result


def readPickleTest():
    """
    TODO you should not call pickle directly, rather load the file into memory and submit the string to our
    function as defined in SemDiscUtils.py

    This is for test purposes only. The createCypherQueries(edgeList) will be called from SemDiscUtils.py
    which in turn returns a string of cypher queries

    :return:
    """
    """ edgeList1 covers this typr of question - "What is the email id of the reader who reads Batman Hush" """
    edgeList1 = {'_node1': {'Name': 'Batman Hush'},
                 '_node2': {'Name': 'Email_Id'},
                 '_attrs': {'Name': 'reads1'}}

    """ edgeList2 covers this typr of question - "What is the email id of Raghuram Rajan" """
    edgeList2 = {'_node1': {'Name': 'Raghuram Rajan'},
                 '_node2': {'Name': 'Phone'}}

    """ edgeList3 covers this typr of question - "Who is Harsha Phulwani" """
    edgeList3 = {'_node1': {'Name': 'Harsha Phulwani'}}

    return edgeList1


if __name__ == '__main__':

    edge = readPickleTest()
    finalQuery = createSPARQLQueries(edge)
    store = sparqlstore.SPARQLWrapper('http://linguistic.technology:5820/books_db1/query')
    store.setCredentials('admin','admin')

    store.setQuery(finalQuery)
    store.setReturnFormat(JSON)
    result1 = store.query().convert()
    for result in result1["results"]["bindings"]:
        if('_node2' in edge):
            print(result[edge['_node2']['Name']]["value"])
        else:
            print(result['Thing']["value"])


