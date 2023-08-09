from connection import Neo4jConnection
from datetime import datetime

class Company(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/companies GET Fetch all companies based on specified tags
    def find_all_companies(self, industry_list=None):
        # Validate input
        if industry_list:
            for industry in industry_list:
                if not industry.isalpha():  # Validate if the industry name is alphanumeric
                    raise ValueError("Invalid industry name")

        query = "MATCH (n:company"
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"
        query += ") RETURN n"
        return self.run_query(query)

    # api/companies POST Add a new company to the database
    def add_company(self, company_name, description, email, contact_num, industries=[]):
        # Create a list of labels formatted as strings
        formatted_industries = [f":{industry}" for industry in industries]

        query = (
            "CREATE (company:company%s {"
            "company_name: $company_name,"
            "description: $description,"
            "email: $email,"
            "contact_num: $contact_num"
            "})"
        ) % "".join(formatted_industries)  # Add labels using CREATE clause

        parameters = {
            "company_name": company_name,
            "description": description,
            "email": email,
            "contact_num": contact_num
        }
        result = self.run_query(query, parameters)
        return result

URI = "neo4j+s://55770697.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "D8BcIOw4QIFj-v7E1QgRAm71kcQ0cLlDx_u3pB4YauQ"

neo4j_client = Company(URI, USER, PASSWORD)
neo4j_client.add_company("Canon", "Canon started out in 1937 with the vision to make the best camera for the world.", "canon@gmail.com", 67998686)