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
            "// Wrote"
            "LOAD CSV WITH HEADERS FROM 'file:///wrote.csv' AS row "
            "MATCH (a:Author {id: row.authorid})"
            "MATCH (p:Paper {id: row.paperid})"
            "MERGE (a)-[:Wrote]->(p);"
            
            "// Reviewed"
            "LOAD CSV WITH HEADERS FROM 'file:///reviwed.csv' AS row "
            "MATCH (a:Author {id: row.authorid})"
            "MATCH (p:Paper {id: row.paperid})"
            "MERGE (a)-[:Reviewed]->(p);"

            "// Corresponding"
            "LOAD CSV WITH HEADERS FROM 'file:///corresponding.csv' AS row "
            "MATCH (a:Author {id: row.authorid})"
            "MATCH (p:Paper {id: row.paperid})"
            "MERGE (a)-[:Corresponding]->(p);"

            "// Published_in (author - edition)"
            "LOAD CSV WITH HEADERS FROM 'file:///published_in_edition.csv' AS row "
            "MATCH (a:Author {id: row.authorid})"
            "MATCH (e:Edition {id: row.editionid})"
            "MERGE (a)-[:Published_in]->(e);"

            "// Has (paper - keyword)"
            "LOAD CSV WITH HEADERS FROM 'file:///has_keyword.csv' AS row "
            "MATCH (p:Paper {id: row.paperid})"
            "MATCH (k:Keyword {id: row.keywordid})"
            "MERGE (p)-[:Has]->(k);"

            "// Cites"
            "LOAD CSV WITH HEADERS FROM 'file:///cites.csv' AS row "
            "MATCH (p1:Paper {id: row.paper1id})"
            "MATCH (p2:Paper {id: row.paper2id})"
            "MERGE (p1)-[:Cites]->(p2);"

            "// Has (proceeding - edition)"
            "LOAD CSV WITH HEADERS FROM 'file:///has_edition.csv' AS row "
            "MATCH (p:Proceeding {id: row.proceedingid})"
            "MATCH (e:Edition {id: row.editionid})"
            "MERGE (p)-[:Has]->(e);"

            "// Happened_in"
            "LOAD CSV WITH HEADERS FROM 'file:///happened_in.csv' AS row "
            "MATCH (e:Edition {id: row.editionid})"
            "MATCH (y:Year {id: row.year})"
            "MERGE (e)-[:Happened_in]->(y);"

            "// Published_in (volume - year)"
            "LOAD CSV WITH HEADERS FROM 'file:///published_in_year.csv' AS row "
            "MATCH (v:Volume {id: row.volumeid})"
            "MATCH (y:Year {id: row.year})"
            "MERGE (v)-[:Published_in]->(y);"

            "// Contains"
            "LOAD CSV WITH HEADERS FROM 'file:///contains.csv' AS row "
            "MATCH (v:Volume {id: row.volumeid})"
            "MATCH (p:Paper {id: row.year})"
            "MERGE (v)-[:Contains]->(p);"

            "// Has (journal - volume)"
            "LOAD CSV WITH HEADERS FROM 'file:///has_volume.csv' AS row "
            "MATCH (j:Journal {id: row.journalid})"
            "MATCH (v:Volume {id: row.volumeid})"
            "MERGE (j)-[:Has]->(v);"

            "// Published_in (paper - year)"
            "LOAD CSV WITH HEADERS FROM 'file:///paper_published_in_year.csv' AS row "
            "MATCH (p:Paper {id: row.paperid})"
            "MATCH (y:Year {id: row.year})"
            "MERGE (p)-[:Published_in]->(y);"
        )
        tx.run(query)
    


if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.load_nodes()
    app.load_edges()
    app.close()