# Driver import
from neo4j import GraphDatabase

class Neo4jClient:
    def __init__(self, uri, username, password):
        self._driver = GraphDatabase.driver(uri, auth=(username, password))
        self._driver.verify_connectivity() 

    def close(self):
        self._driver.close()

    def run_query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result.data()
    
    # api/journalists GET Fetch all journalists based on specified tags
    def find_all_journalists():
        query = "MATCH (n:journalist) RETURN n"
        neo4j_client.run_query(query)

    # api/journalists POST Add a new journalist to the database
    def add_journalist(self, first_name, last_name, birthdate, description, email, mobile_num, industries):
        query = (
        "CREATE (journalist:journalist {"
        "first_name: $first_name,"
        "last_name: $last_name,"
        "birthdate: $birthdate,"
        "description: $description,"
        "email: $email,"
        "mobile_num: $mobile_num,"
        "industries: $industries"
        "})"
        )
        result = self.run_query(query, parameters)

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://55770697.databases.neo4j.io"
USER = "neo4j"
AUTH = "D8BcIOw4QIFj-v7E1QgRAm71kcQ0cLlDx_u3pB4YauQ"

neo4j_client = Neo4jClient(URI, USER, AUTH)
neo4j_client.find_all_journalists()

# api/journalists/{id} GET Fetch a journalist based on ID

# api/journalists/{id} PUT Update a journalist based on ID

# api/journalists/{id} PUT Update a journalist’s tags

# api/journalists/{id}/history POST Add a new employment record for a journalist

# api/journalists/{id}/history GET Get all employment records for a journalist

# api/journalists/{id}/notes GET Get all notes for a journalist

# api/journalists/{id}/notes POST Add a new note for a journalist