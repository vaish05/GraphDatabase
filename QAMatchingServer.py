#!/usr/bin/env python3

"""
QAMatching.py used for this QAMatchingServer.py

Created by Sriram Sitharaman, Damir Cavar

Version: 0.1


Given a input Natural Language query,
Identifies the matching regex and hits the neo4j graph DB with the corresponding Cypher query

11/30/2017 : Created by Sriram Sitharaman
12/3/2017  : Updated By Shridivya Sharma - Updated date functions

To run the code in test mode one server instance at the configured port has to be present. The test mode uses a client
to communicate via XML-RPC to the server.

Run a server first in a terminal:

    python3 QAMatchingServer.py -c config.ini

Then run the test client in another terminal window, and observe the logs and outputs.

    python3 QAMatchingServer.py -t -c config.ini

"""

import re, sys, argparse, configparser, logging, time
from neo4j.v1 import GraphDatabase, basic_auth
# from py2neo import authenticate, Graph
from bs4 import BeautifulSoup
import wikipedia
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from SemDiscUtils import encodeReturn, decodeReturn
from defaults import MODULES, CONFFILE


MODULENAME = "QAMatchingServer"


def loadModel():
    """Loads the XML model of patterns and queries and returns the two lists."""

    # Reading the XML file content in to a variable
    # regexXML = open("QAPatterns.xml").read()
    # Using BeautifulSoup to parse the loaded XML file
    parsedXML = BeautifulSoup(open("QAPatterns.xml").read(), "lxml")

    # Getting the List of regex patterns from the XML file
    patternsList = [patternTag.text for patternTag in parsedXML.findAll("pattern")]

    # Getting the List of corresponding Cypher query matches
    cypherList = [cypherTag.text for cypherTag in parsedXML.findAll("cypher")]

    return patternsList, cypherList


def parse(text):
    """Identifies the matching regex and hits the neo4j graph DB with the corresponding Cypher query"""

    text = text.replace("?", "")
    hit_yes_no = False

    #if log:
    logging.debug("Question:" + text)
    #else:
    print("Question:", text)

    uri = "bolt://linguistic.technology:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "DtwAMjrk6zt1bHifYOJ6"))
    session = driver.session()
    # match (n) optional match (n)-[r]-() return n,r:
    # graph = Neo4J.createCypherQueries(self._edgeList)
    # authenticate("linguistic.technology:7474", "neo4j", "DtwAMjrk6zt1bHifYOJ6")
    # graph = Graph("http://linguistic.technology:7474/db/data/")

    result = ""
    matchedX = ""

    pos = 0
    for pos in range(len(patternsList)):
        match = re.match(patternsList[pos], text, re.IGNORECASE)
        if match:
            matchedX = list(match.groups())[-1]
            print("matchedX:", matchedX)
            hit_yes_no = True
            #logging.debug("Match:" + matchedX)
            print("Match:", matchedX)
            break

    if hit_yes_no:
        cypher = cypherList[pos].replace("(.*)", matchedX)
        logging.debug("Cypher:\n" + cypher)
        print("Cypher:", cypher)
        #result = graph.cypher.execute(cypher)
        result = session.run(cypher)
        logging.debug("Result:")
        logging.debug(result)
        print("Result:", result)
        res=[record for record in result]
        if len(res)>0:
            Count=0
            resultStr=[]
            for row in res:
                StartName=row['a'].properties['Name']
                StartLabel=row['a'].properties['label']
                try:
                    relationshipName=row['r'].properties['Name']
                    EndName=row['b'].properties['Name']
                    EndLabel=row['b'].properties['label']
                except:
                    relationshipName=EndName=EndLabel=""
                if Count==0:
                    resultStr.append(" ".join([StartName,"is a",StartLabel]))
                if len(relationshipName)!=0:
                    resultStr.append(" ".join([StartName,relationshipName,EndName]))
                Count+=1
            session.close()
            return ". ".join(resultStr)
        # Hits Wikipedia only for factual questions
        else: #  and pos <= 2:
            resultStr=[]
            logging.debug("Result from Wikipedia:")
            print("Here is what we found on Wikipedia:")
            resultStr.append("Here is what we found on Wikipedia:")
            # changed code:
            #
            try:
                result = wikipedia.page(matchedX)
            except:
                return "Sorry, I cannot help you with that."
            #print("Wikipedia suggests for:", matchedX)
            #print(result)
            #if result:
            #    result = str(wikipedia.page(result).summary.encode(sys.stdout.encoding, 'ignore')) # .split(".")[:2]
            #    print("Wikipedia:")
            #    print(result)
            #else:
            #    result = wikipedia.search(matchedX)
            #    if result:
            #        result = result[0]
            #    print("Wikipedia search result:")
            #    print(result)
            if result:
                resultStr.append(". ".join(result.summary.split(".")[:2])) # .encode("utf-8").split(".")[:2]))
            logging.debug(resultStr)
            print(".".join(resultStr))
            # session.close()
            return ". ".join(resultStr)

    logging.debug("No Matches found!")
    print ("No Matches found!")
    return "Sorry, I cannot help you with that."
    # close Neo4J session

    #print("Type of result:", type(result))
    #print("Test", list(result))
    ##for record in result:
    ##    print("BoltStatementResult:")
    ##    print(record["a"], record["r"], record["b"])
    #logging.debug("-----------------------------------------------------------------------")
    #print("-----------------------------------------------------------------------")
    #print("")
    #return ".".join(resultStr)


def test():
    # Process some sample questions
    for x in ("Who is Obama?",
              "Who likes cherries?",
              "Who bought Apple?",
              "Tell me about Peter",
              "Who is the president of the United States of America?"):
        start = time.time()
        s = xmlrpc.client.ServerProxy("http://{}:{}".format(MODULES[MODULENAME].host, MODULES[MODULENAME].port), allow_none=True)
        res = s.parse(x)
        if res:
            print("Result:")
            print(res)
        print("Processing time: ", time.time() - start)
        print("Result:", res)


def parseConfiguration(conff=CONFFILE):
    """Parse the config.ini and set the parameters."""
    global MODULES

    # read configuration
    config = configparser.ConfigParser()
    config.read(conff)

    # parse config for modules
    for l in ("QAMatchingServer", "Neo4J", "StarDog"):
        if l not in MODULES.keys():
            continue
        inilabel = MODULES[l].inilabel
        if inilabel in config.sections():
            if "host" in config[inilabel]:
                MODULES[l].host = config[inilabel]["host"]
            if "port" in config[inilabel]:
                MODULES[l].port = int(config[inilabel]["port"])
            if "logfile" in config[inilabel]:
                MODULES[l].logfile = config[inilabel]["logfile"]


def mainServer(port, host):
    """Start the Dispatcher in Server mode."""

    print("Host:", host, "Port:", port)

    # start the XML-RPC server
    server = SimpleXMLRPCServer((host, int(port)), allow_none=True)
    server.register_introspection_functions()
    server.register_function(parse)

    logging.info('Serving over XML-RPC on {} port {}'.format(host, port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Interrupt received, exiting.")
        sys.exit(0)


if __name__ == "__main__":
    # command line arguments overwrite config-file parameters
    parser = argparse.ArgumentParser(prog=MODULENAME, description='Command line arguments.', epilog='')
    parser.add_argument('-c', '--config', dest="conffile", default=CONFFILE,
                        help="Alternative " + CONFFILE + " file name")
    parser.add_argument('-t', '--test', dest='test', action='store_true', help="Run in test mode")  # just a flag
    args = parser.parse_args()

    if args.conffile != CONFFILE:
        parseConfiguration(args.conffile)
    else:
        parseConfiguration()

    # start logging
    logging.basicConfig(filename=MODULES[MODULENAME].logfile,
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)

    # loads the model
    patternsList, cypherList = loadModel()

    if args.test:
        test()
    else:
        mainServer(MODULES[MODULENAME].port, MODULES[MODULENAME].host)
