from connection import Neo4jConnection
from datetime import datetime

class Journalist(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/journalists GET Fetch all journalists in specified industries
    def find_all_journalists(self, industry_list=None):
        # Validate input
        if industry_list:
            for industry in industry_list:
                if not industry.isalpha():  # Validate if the industry name is alphanumeric
                    raise ValueError("Invalid industry name")

        query = "MATCH (n:journalist"
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"
        query += ") RETURN n"
        return self.run_query(query)

    # api/journalists POST Add a new journalist to the database
    def add_journalist(self, journalist_attributes):
        # Format the birthdate str
        birthdate = datetime.strptime(journalist_attributes.get("birthdate"), "%Y-%m-%d").date()

        # Extract attributes from the JSON object
        parameters = {
            "first_name": journalist_attributes.get("first_name"),
            "last_name": journalist_attributes.get("last_name"),
            "birthdate": birthdate,
            "description": journalist_attributes.get("description"),
            "email": journalist_attributes.get("email"),
            "mobile_num": journalist_attributes.get("mobile_num")
        }

        # Create a list of labels formatted as strings
        industries = journalist_attributes.get("industries", [])
        formatted_industries = [f":{industry}" for industry in industries]

        query = (
            "CREATE (journalist:journalist {"
            "first_name: $first_name,"
            "last_name: $last_name,"
            "birthdate: date($birthdate),"
            "description: $description,"
            "email: $email,"
            "mobile_num: $mobile_num"
            "})"
            "SET journalist" + "".join(formatted_industries)  # Add labels using SET clause
        )
        result = self.run_query(query, parameters)
        return result

# api/journalists/{id} GET Fetch a journalist based on name
    def fuzzy_search_journalists_by_name(self, name, max_distance=3):
            query = (
                "MATCH (journalist:journalist) "
                "WITH journalist, "
                "     apoc.text.distance(toLower(journalist.first_name), toLower($name)) AS fn_distance, "
                "     apoc.text.distance(toLower(journalist.last_name), toLower($name)) AS ln_distance "
                "WHERE fn_distance <= $max_distance OR ln_distance <= $max_distance "
                "RETURN journalist, fn_distance, ln_distance "
                "ORDER BY fn_distance, ln_distance"
            )
            parameters = {"name": name, "max_distance": max_distance}
            return self.run_query(query, parameters)

# api/journalists/{id} PUT Update a journalist's personal details
    def update_journalist_properties(self, journalist_id, new_properties):
        query = (
            "MATCH (journalist:journalist) "
            "WHERE ID(journalist) = $journalist_id "
            "SET journalist += $new_properties "
            "RETURN journalist"
        )
        parameters = {"journalist_id": journalist_id, "new_properties": new_properties}
        return self.run_query(query, parameters)

# api/journalists/{id} PUT Update a journalist’s industries
    def add_journalist_industries(self, journalist_id, new_industry_list):
        query = f"MATCH (journalist) WHERE ID(journalist) = {journalist_id} SET journalist:{':'.join(new_industry_list)}"
        return self.run_query(query)

    def remove_journalist_industries(self, journalist_id, industries_to_remove):
        industries_to_remove_str = ":".join(industries_to_remove)
        query = f"MATCH (journalist) WHERE ID(journalist) = {journalist_id} REMOVE journalist:{industries_to_remove_str}"
        return self.run_query(query)

# api/journalists/{id}/history POST Add a new employment record for a journalist
# should include role, start date and end date
    def add_employment_record_for_journalist(self, journalist_id, company_id, relationship_properties):
        # Validate input IDs
        if not isinstance(journalist_id, int) or not isinstance(company_id, int):
            raise ValueError("Node IDs must be integers.")
        
        # Validate relationship properties
        if not isinstance(relationship_properties, dict):
            raise ValueError("Relationship properties must be a dictionary.")

        # Validate that the nodes are journalist and company nodes respectively
        is_journalist = self.node_has_label(journalist_id, "journalist")
        is_company = self.node_has_label(company_id, "company")

        if (is_journalist == False or is_company == False):
            raise ValueError("Nodes are the wrong type.")
        
        query = (
            "MATCH (journalist), (company) "
            "WHERE ID(journalist) = $journalist_id AND ID(company) = $company_id "
            "CREATE (journalist)-[r:%s]->(company) SET r = $relationship_properties" % "employed"
        )
        parameters = {
            "journalist_id": journalist_id,
            "company_id": company_id,
            "relationship_properties": relationship_properties
        }
        self.run_query(query, parameters)

# api/journalists/{id}/history GET Get all employment records for a journalist
    def get_all_employment_records(self, specific_node_id):
        query = (
            "MATCH (journalist)-[employment:EMPLOYMENT]->(company) "
            "WHERE ID(journalist) = $journalist_id "
            "RETURN employment, company"
        )
        parameters = {"journalist_id": journalist_id}
        result = self.run_query(query, parameters)
        return result

# api/journalists/{id}/notes GET Get all notes for a journalist
    def get_all_notes(self, journalist_id):
            query = (
                "MATCH (source_node)-[:CONNECTED_TO]->(target_node:%s) "
                "WHERE ID(source_node) = $journalist_id "
                "RETURN target_node" % "note"
            )
            parameters = {"journalist_id": journalist_id}
            result = self.run_query(query, parameters)
            return result

# api/journalists/{id}/notes POST Add a new note for a journalist
    def create_new_note_for_journalist(self, journalist_id, note_properties):
        query = (
            "MATCH (journalist) "
            "WHERE ID(journalist) = $journalist_id "
            "CREATE (note:note $note_properties) "
            "CREATE (journalist)-[:HAS_NOTE]->(note) "
            "RETURN note"
        )

        parameters = {
            "journalist_id": journalist_id,
            "note_properties": note_properties
        }
        result = self.run_query(query, parameters)
        return result
    
# Check if a node has a specific label
    def node_has_label(self, node_id, label):
        query = (
            "MATCH (node) "
            "WHERE ID(node) = $node_id AND $label IN labels(node) "
            "RETURN COUNT(node) > 0 AS has_label"
        )
        parameters = {"node_id": node_id, "label": label}
        result = self.run_query(query, parameters)

        if result:
            return True
        else:
            return False