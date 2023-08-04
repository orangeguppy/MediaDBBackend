# Driver import
from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://55770697.databases.neo4j.io"
AUTH = ("neo4j", "D8BcIOw4QIFj-v7E1QgRAm71kcQ0cLlDx_u3pB4YauQ")

with GraphDatabase.driver(URI, auth=AUTH) as driver: 
    driver.verify_connectivity() 

# api/journalists GET Fetch all journalists based on specified tags
def find_all_journalists():
    match (person:journalist)
    return person.title


