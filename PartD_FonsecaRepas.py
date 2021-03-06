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

    def setup_recommender(self):
        with self.driver.session() as session:
            session.write_transaction(self._create_database_community)
            session.write_transaction(self._connect_community_keywords)
            session.write_transaction(self._relate_conferences_to_community)
            session.write_transaction(self._relate_journals_to_community)
            session.write_transaction(self._highlight_top100)

    def recommend_reviewers(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_reviewers)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)

    def recommend_gurus(self):
        with self.driver.session() as session:
            result = session.write_transaction(self._get_gurus)
            print("\n Showing first 10 rows of the result \n")
            for row in result[:10]:
                print(row)
            
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

    @staticmethod
    def _relate_conferences_to_community(tx):
        query = (
            "MATCH (conference:Conference)-[:Has]->(:Edition)<-[:Published_in]-(paper:Paper) "
            "WITH conference, COUNT (paper) AS total_papers "
            "MATCH (conference)-[:Has]->(:Edition)<-[:Published_in]-(community_paper:Paper)-[:Has]->(:Keyword)<-[:Contains]-(:Community {name: 'database'}) "
            "WITH conference, COUNT(community_paper) AS total_com_papers, total_papers "
            "WITH conference, total_com_papers * 1.0 / total_papers AS community_percentual"
            "WHERE community_percentual >= 0.03 "
            "MATCH (community:Community {name: 'database'}) "
            "CREATE (conference)-[:Relates]->(community); "
            )
        tx.run(query)

    @staticmethod
    def _relate_journals_to_community(tx):
        query = (
            "MATCH (journal:Journal)-[:Has]->(:Volume)-[:Contains]->(paper:Paper) "
            "WITH journal, COUNT (paper) AS total_papers "
            "MATCH (journal:Journal)-[:Has]->(:Volume)-[:Contains]->(community_paper:Paper)-[:Has]->(:Keyword)<-[:Contains]-(:Community {name: 'database'}) "
            "WITH journal, COUNT(community_paper) AS total_com_papers, total_papers "
            "WITH journal, total_com_papers * 1.0 / total_papers AS community_percentual "
            "WHERE community_percentual >= 0.03 "
            "MATCH (community:Community {name: 'database'}) "
            "CREATE (journal)-[:Relates]->(community); "
            )
        tx.run(query)

    @staticmethod
    def _create_database_community_graph(tx):
        query = (
            "CALL gds.graph.create.cypher("
                "\"database_community\", "
                "\"MATCH (n:Paper)-[r]-()<--()-[:Relates]->(:Community { name: \\\"database\\\"}) WHERE r:Contains OR r:Published_in RETURN DISTINCT id(n) AS id\", "
                "\"MATCH (n)-[r:Cites]->(p) RETURN id(n) AS source, id(p) AS target\", "
                "{ validateRelationships: false }"
            ");"
            )
        tx.run(query)

    @staticmethod
    def _highlight_top100(tx):
        query = (
            "CALL gds.pageRank.stream(\"database_community\")"
            "YIELD nodeId, score"
            "WITH gds.util.asNode(nodeId) AS paper"
            "ORDER BY score DESC"
            "LIMIT 100"
            "SET paper :Top100DatabaseCommunity"
            "RETURN paper;"
            )
        tx.run(query)

    @staticmethod
    def _get_reviewers(tx):
        query = (
            "MATCH (potential_reviewer:Author)-[:Wrote]->(n:Top100DatabaseCommunity)"
            "RETURN DISTINCT potential_reviewer;"
            )
        result = tx.run(query)
        return result

    @staticmethod
    def _get_gurus(tx):
        query = (
            "MATCH (guru:Author)-[:Wrote]->(p1:Top100DatabaseCommunity)"
            "MATCH (guru:Author)-[:Wrote]->(p2:Top100DatabaseCommunity)"
            "WHERE p1 <> p2"
            "RETURN DISTINCT guru;"
            )
        result = tx.run(query)
        return result
        
        

if __name__ == "__main__":
    bolt_url = "bolt://localhost:7687"
    user = "neo4j"
    password = "sdm123"
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_url, user, password)
    app.setup_recommender()
    app.recommend_reviewers()
    app.recommend_gurus()
    app.close()