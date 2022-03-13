import logging
import sys

from neo4j import GraphDatabase

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

    def add_reviews(self):
        with self.driver.session() as session:
            session.write_transaction(self._add_reviews)

    @staticmethod
    def _add_reviews(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///reviewed_v2.csv' AS row "
            "MATCH (:Author {id: row.authorid})-[r:Reviewed]->(:Paper {id: row.paperid})"
            "SET r.content = row.review, r.decision = row.suggested_decision;"
        )
        tx.run(query)
        print("Edge (author)-[REVIEWED]->(paper) updated")

    def add_affiliations(self):
        with self.driver.session() as session:
            session.write_transaction(self._add_affiliations)

        with self.driver.session() as session:
            session.write_transaction(self._create_index_affiliationid)

        with self.driver.session() as session:
            session.write_transaction(self._add_affiliated)

    @staticmethod
    def _add_affiliations(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///affiliation.csv' AS row "
            "CREATE (:Affiliation {id: row._id, name: row.name});"
            )
        tx.run(query)
        print("Affiliations loaded")

    @staticmethod
    def _create_index_affiliationid(tx):
        query = ("CREATE INDEX affiliationid_index FOR (n:Affiliation) ON (n.id)")
        tx.run(query)
        print("Created index on Affiliation.id")

    @staticmethod
    def _add_affiliated(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///affiliated.csv' AS row "
            "MATCH (au:Author {id: row.authorid})"
            "MATCH (af:Affiliation {id: row.affiliationid})"
            "CREATE (au)-[:Affiliated]->(af);"
            )
        tx.run(query)
        print("Edge (author)-[AFFILIATED]->(affiliation) loaded")


if __name__ == "__main__":
    bolt_url = "bolt://localhost:11005"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.add_reviews()
    app.add_affiliations()
    app.close()