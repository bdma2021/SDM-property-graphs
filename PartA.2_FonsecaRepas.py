import logging
import sys

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def clean_db(self):
        with self.driver.session() as session:
            session.write_transaction(self._clean_db)

    @staticmethod
    def _clean_db(tx):
        query = ("MATCH (n) DETACH DELETE n;")
        tx.run(query)

    def load_nodes(self):
        with self.driver.session() as session:
            session.write_transaction(self._load_nodes)

    @staticmethod
    def _load_nodes(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row "
            "CREATE (:Author {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///editions.csv' AS row "
            "CREATE (:Edition {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///journals.csv' AS row "
            "CREATE (:Journal {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' AS row "
            "CREATE (:Keyword {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row "
            "CREATE (:Paper {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///proceedings.csv' AS row "
            "CREATE (:Proceeding {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///volumes.csv' AS row "
            "CREATE (:Volume {name: row.name});"
            "LOAD CSV WITH HEADERS FROM 'file:///years.csv' AS row "
            "CREATE (:Year {name: row.name});"
        )
        tx.run(query)

    def load_edges(self):
        with self.driver.session() as session:
            session.write_transaction(self._load_edges)

    @staticmethod
    def _load_edges(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///wrote.csv' AS row "
            "MATCH (a:Author {id: row.authorid})"
            "MATCH (p:Paper {id: row.paperid})"
            "MERGE (a)-[:Wrote]->(p);"
            # TODO: put the rest of the edges here once we have the data
        )
        tx.run(query)
    


if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.load_companies()
    app.close()