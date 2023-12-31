from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, username, password):
        self._driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self._driver.close()

    def run_query(self, query, parameters=None):
        try:
            with self._driver.session() as session:
                result = session.run(query, parameters=parameters)
                records = list(result)  # Fetch all records and store them in a list

        except Exception as e:
            print("Error:", e)
        finally:
            if self._driver:
                self._driver.close()
                return records