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

    
    def paper_similarity(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_bipartite_graph)
            result = session.write_transaction(self._compute_similarity)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)

    @staticmethod
    def _create_bipartite_graph(tx):
        query = ("CALL gds.graph.create('citation_network ', 'Paper', 'Cites');")
        tx.run(query)

    @staticmethod
    def _compute_similarity(tx):
        query = (
            "CALL gds.pageRank.stream('citation_network')"
            "YIELD nodeId, score"
            "RETURN gds.util.asNode(nodeId) AS paper, score"
            "ORDER BY score DESC;"
        )
        result = tx.run(query)
        return [{'paper': row['paper'], 'score': row['score']} for row in result]


if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.paper_similarity()
    app.close()
