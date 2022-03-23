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

    
    def find_top3_papers_of_conference(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_conference_communities)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)

    @staticmethod
    def _find_top3_papers_of_conference(tx):
        query = (
            "MATCH (:Paper)-[c:Cites]->(p:Paper)<-[:Wrote]-(author:Author)"
            "WITH author, p, COUNT(c) AS citations"
            "ORDER BY citations DESC"
            "WITH author, collect({paper:p, citations:citations}) AS paper_citations"
            "UNWIND range(1,size(paper_citations)) AS i"
            "WITH author, paper_citations[i-1].citations AS citations, i"
            "WHERE citations > i"
            "RETURN author, max(i) AS h_index"
        )
        result = tx.run(query)
        return [{'author': row['author']['name'], 'h_index': row['h_index']} for row in result]

if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.find_conference_communities()
    app.close()
