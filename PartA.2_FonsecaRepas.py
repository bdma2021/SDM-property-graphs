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

    def clean_db(self):
        with self.driver.session() as session:
            session.write_transaction(self._clean_db)

    @staticmethod
    def _clean_db(tx):
        query = ("MATCH (n) DETACH DELETE n;")
        tx.run(query)
        print("Database emptied")

    def load_nodes(self):
        with self.driver.session() as session:
            session.write_transaction(self._load_authors)
            session.write_transaction(self._load_editions)
            session.write_transaction(self._load_journals)
            session.write_transaction(self._load_keywords)
            session.write_transaction(self._load_papers)
            session.write_transaction(self._load_conferences)
            session.write_transaction(self._load_volumes)
            session.write_transaction(self._load_years)

    @staticmethod
    def _load_authors(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row "
            "CREATE (:Author {id: row._id, name: row.name});"
            )
        tx.run(query)
        print("Authors loaded")

    @staticmethod
    def _load_editions(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///editions.csv' AS row "
            "CREATE (:Edition {id: row._id, name: row.name, number: row.number, city: row.city});"
            )
        tx.run(query)
        print("Editions loaded")

    @staticmethod
    def _load_journals(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///journals.csv' AS row "
            "CREATE (:Journal {id: row._id, name: row.name});"
            )
        tx.run(query)
        print("Journals loaded")

    @staticmethod
    def _load_keywords(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///keywords.csv' AS row "
            "CREATE (:Keyword {id: row._id, keyword: row.keyword});"
            )
        tx.run(query)
        print("Keywords loaded")

    @staticmethod
    def _load_papers(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row "
            "CREATE (:Paper {id: row._id, title: row.title, language: row.lang, isbn: row.isbn, abstract: row.abstract});"
            )
        tx.run(query)
        print("Papers loaded")

    @staticmethod
    def _load_conferences(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///conferences.csv' AS row "
            "CREATE (:Conference {id: row._id, name: row.name});"
            )
        tx.run(query)
        print("Conferences loaded")

    @staticmethod
    def _load_volumes(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///volumes.csv' AS row "
            "CREATE (:Volume {id: row._id, title: row.title});"
            )
        tx.run(query)
        print("Volumes loaded")
        
    @staticmethod
    def _load_years(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///years.csv' AS row "
            "CREATE (:Year {year: row.year});"
            )
        tx.run(query)
        print("Years loaded")

    def create_indexes(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_index_authorid)
            session.write_transaction(self._create_index_editionid)
            session.write_transaction(self._create_index_journalid)
            session.write_transaction(self._create_index_keywordid)
            session.write_transaction(self._create_index_paperid)
            session.write_transaction(self._create_index_conferenceid)
            session.write_transaction(self._create_index_volumeid)
            session.write_transaction(self._create_index_year)

    @staticmethod
    def _create_index_authorid(tx):
        query = ("CREATE INDEX authorid_index FOR (n:Author) ON (n.id)")
        tx.run(query)
        print("Created index on Author.id")
    
    @staticmethod
    def _create_index_editionid(tx):
        query = ("CREATE INDEX editionid_index FOR (n:Edition) ON (n.id)")
        tx.run(query)
        print("Created index on Edition.id")

    @staticmethod
    def _create_index_journalid(tx):
        query = ("CREATE INDEX journalid_index FOR (n:Journal) ON (n.id)")
        tx.run(query)
        print("Created index on Journal.id")

    @staticmethod
    def _create_index_keywordid(tx):
        query = ("CREATE INDEX keywordid_index FOR (n:Keyword) ON (n.id)")
        tx.run(query)
        print("Created index on Keyword.id")

    @staticmethod
    def _create_index_paperid(tx):
        query = ("CREATE INDEX paperid_index FOR (n:Paper) ON (n.id)")
        tx.run(query)
        print("Created index on Paper.id")

    @staticmethod
    def _create_index_conferenceid(tx):
        query = ("CREATE INDEX conferenceid_index FOR (n:Conference) ON (n.id)")
        tx.run(query)
        print("Created index on Conference.id")

    @staticmethod
    def _create_index_volumeid(tx):
        query = ("CREATE INDEX volumeid_index FOR (n:Volume) ON (n.id)")
        tx.run(query)
        print("Created index on Volume.id")

    @staticmethod
    def _create_index_year(tx):
        query = ("CREATE INDEX year_index FOR (n:Year) ON (n.year)")
        tx.run(query)
        print("Created index on Year.year")


    def load_edges(self):
        with self.driver.session() as session:
            session.write_transaction(self._load_author_wrote_paper)
            session.write_transaction(self._load_author_reviewed_paper)
            session.write_transaction(self._load_author_corresponding_paper)
            session.write_transaction(self._load_paper_has_keywords)
            session.write_transaction(self._load_paper_cites_paper)
            session.write_transaction(self._load_conference_has_edition)
            session.write_transaction(self._load_edition_happened_in_year)
            session.write_transaction(self._load_volume_published_in_year)
            session.write_transaction(self._load_volume_contains_paper)
            session.write_transaction(self._load_journal_has_volume)
            session.write_transaction(self._load_paper_published_in_edition)
            session.write_transaction(self._load_author_published_in_edition)
            

    @staticmethod
    def _load_author_wrote_paper(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///wrote.csv' AS row "
            "MATCH (a:Author {id: row.authorid}) "
            "MATCH (p:Paper {id: row.paperid}) "
            "CREATE (a)-[:Wrote]->(p);"
            )
        tx.run(query)
        print("Edge (author)-[WROTE]->(paper) loaded")
    
    @staticmethod
    def _load_author_reviewed_paper(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///reviewed.csv' AS row "
            "MATCH (a:Author {id: row.authorid}) "
            "MATCH (p:Paper {id: row.paperid}) "
            "CREATE (a)-[:Reviewed]->(p);"
            )
        tx.run(query)
        print("Edge (author)-[REVIEWED]->(paper) loaded")

    @staticmethod
    def _load_author_corresponding_paper(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///corresponding.csv' AS row "
            "MATCH (a:Author {id: row.authorid}) " 
            "MATCH (p:Paper {id: row.paperid}) "
            "CREATE (a)-[:Corresponding]->(p);"
            )
        tx.run(query)
        print("Edge (author)-[CORRESPONDING]->(paper) loaded")

    @staticmethod
    def _load_author_published_in_edition(tx):
        query = (
            "MATCH (a:Author)-[:Wrote]->(p:Paper)-[:Published_in]->(e:Edition)"
            "CREATE (a)-[:Published_in]->(e)"
            )
        tx.run(query)
        print("Edge (author)-[PUBLISHED_IN]->(edition) loaded")

    @staticmethod
    def _load_paper_has_keywords(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///has_keyword.csv' AS row "
            "MATCH (p:Paper {id: row.paperid}) "
            "MATCH (k:Keyword {id: row.keywordid}) "
            "CREATE (p)-[:Has]->(k);"
            )
        tx.run(query)
        print("Edge (paper)-[HAS]->(keyword) loaded")

    @staticmethod
    def _load_paper_cites_paper(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///cites.csv' AS row "
            "MATCH (p1:Paper {id: row.paperid}) "
            "MATCH (p2:Paper {id: row.referenceid}) "
            "CREATE (p1)-[:Cites]->(p2);"
            )
        tx.run(query)
        print("Edge (paper)-[CITES]->(paper) loaded")

    @staticmethod
    def _load_conference_has_edition(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///has_edition.csv' AS row "
            "MATCH (c:Conference {id: row.conferenceid}) "
            "MATCH (e:Edition {id: row.editionid}) "
            "CREATE (c)-[:Has]->(e);"
            )
        tx.run(query)
        print("Edge (conference)-[HAS]->(edition) loaded")

    @staticmethod
    def _load_edition_happened_in_year(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///happened_in.csv' AS row "
            "MATCH (e:Edition {id: row.editionid}) " 
            "MATCH (y:Year {year: row.year}) "
            "CREATE (e)-[:Happened_in]->(y);"
            )
        tx.run(query)
        print("Edge (edition)-[HAPPENED_IN]->(year) loaded")

    @staticmethod
    def _load_volume_published_in_year(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///volume_published_in_year.csv' AS row "
            "MATCH (v:Volume {id: row.volumeid}) "
            "MATCH (y:Year {year: row.year}) "
            "CREATE (v)-[:Published_in]->(y);"
            )
        tx.run(query)
        print("Edge (volume)-[PUBLISHED_IN]->(year) loaded")

    @staticmethod
    def _load_volume_contains_paper(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///contains.csv' AS row "
            "MATCH (v:Volume {id: row.volumeid}) "
            "MATCH (p:Paper {id: row.paperid}) "
            "CREATE (v)-[:Contains]->(p);"
            )
        tx.run(query)
        print("Edge (volume)-[CONTAINS]->(paper) loaded")

    @staticmethod
    def _load_journal_has_volume(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///has_volume.csv' AS row "
            "MATCH (j:Journal {id: row.journalid}) "
            "MATCH (v:Volume {id: row.volumeid}) "
            "CREATE (j)-[:Has]->(v);"
            )
        tx.run(query)
        print("Edge (joural)-[HAS]->(volume) loaded")

    @staticmethod
    def _load_paper_published_in_edition(tx):
        query = (
            "LOAD CSV WITH HEADERS FROM 'file:///published_in_edition.csv' AS row "
            "MATCH (p:Paper {id: row.paperid}) "
            "MATCH (e:Edition {id: row.editionid}) "
            "CREATE (p)-[:Published_in]->(e);"
            )
        tx.run(query)
        print("Edge (paper)-[PUBLISHED_IN]->(year) loaded")


if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.clean_db()
    app.load_nodes()
    app.create_indexes()
    app.load_edges()
    app.close()