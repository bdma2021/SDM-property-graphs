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

    
    def find_conference_communities(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_conference_communities)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)

    @staticmethod
    def _find_conference_communities(tx):
        query = (
            "MATCH (c:Conference)-[:Has]->(e:Edition)<-[:Published_in]-(a:Author) "
            "WITH c AS conference, a AS author, COUNT(DISTINCT e) AS number_editions "
            "RETURN conference, collect(CASE WHEN number_editions >= 4 THEN author END) AS community "
            "ORDER BY size(community) DESC"
        )
        result = tx.run(query)
        return [{'conference': row['conference']['name'], 'community': row['community']} for row in result]

if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.find_conference_communities()
    app.close()