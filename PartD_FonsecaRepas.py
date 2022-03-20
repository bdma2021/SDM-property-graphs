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


    def create_database_community(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_database_community)
            session.write_transaction(self._connect_community_keywords)
            

    @staticmethod
    def _create_database_community(tx):
        query = ("CREATE (:Community {name: 'database'});")
        tx.run(query)

    @staticmethod
    def _connect_community_keywords(tx):
        query = (
            "MATCH (keyword:Keyword) "
            "WHERE toLower(keyword.keyword) IN ['data management', 'database index', 'data modeling', 'big data', 'data processing', 'data store', 'database querying'] "
            "MATCH (community:Community {name: 'database'}) "
            "CREATE (community)-[:Contains]->(keyword); "
            )
        tx.run(query)
        

if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.create_database_community()
    app.close()