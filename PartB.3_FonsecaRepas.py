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

    
    def find_journals_impact_factor(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._find_journals_impact_factor)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)

    @staticmethod
    def _find_journals_impact_factor(tx):
        query = (
            # For year 2019
            "MATCH (journal:Journal)-[:Has]->(v:Volume)-[:Contains]->(p:Paper) "
            "MATCH (v)-[:Published_in]->(publication_year:Year) "
            "MATCH (p)-[:Cites]->(reference:Paper)<-[:Contains]-(:Volume)-[:Published_in]->(citation_year:Year) "
            "WHERE citation_year.year = '2019' AND publication_year.year IN ['2018', '2017'] "
            "RETURN journal, COUNT(DISTINCT reference) * 1.0 / COUNT(DISTINCT p) AS impact_factor "
            "ORDER BY impact_factor DESC;"
        )
        result = tx.run(query)
        return [{'journal': row['journal']['name'], 'impact factor': row['impact_factor']} for row in result]

if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.find_journals_impact_factor()
    app.close()