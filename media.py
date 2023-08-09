from connection import Neo4jConnection
from datetime import datetime

class Media(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/media GET Fetch all media in specified industries
    def find_all_media(self, industry_list=None):
        # Validate input
        if industry_list:
            for industry in industry_list:
                if not industry.isalpha():  # Validate if the industry name is alphanumeric
                    raise ValueError("Invalid industry name")

        query = "MATCH (n:media"
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"
        query += ") RETURN n"
        return self.run_query(query)

    # api/media POST Add a new media to the database
    def add_media(self, first_name, last_name, birthdate, description, email, mobile_num, industries=[]):
        # Format the birthdate str
        birthdate = datetime.strptime(birthdate, "%Y-%m-%d").date()
        
        # Create a list of labels formatted as strings
        formatted_industries = [f":{industry}" for industry in industries]
        
        query = (
            "CREATE (media:media {"
            "first_name: $first_name,"
            "last_name: $last_name,"
            "birthdate: date($birthdate),"
            "description: $description,"
            "email: $email,"
            "mobile_num: $mobile_num"
            "})"
            "SET media" + "".join(formatted_industries)  # Add labels using SET clause
        )
        
        parameters = {
            "first_name": first_name,
            "last_name": last_name,
            "birthdate": birthdate,
            "description": description,
            "email": email,
            "mobile_num": mobile_num
        }
        result = self.run_query(query, parameters)

# api/media/{id} GET Fetch a media based on name
    def fuzzy_search_media_by_name(self, name, max_distance=3):
            query = (
                "MATCH (media:media) "
                "WITH media, "
                "     apoc.text.distance(toLower(media.first_name), toLower($name)) AS fn_distance, "
                "     apoc.text.distance(toLower(media.last_name), toLower($name)) AS ln_distance "
                "WHERE fn_distance <= $max_distance OR ln_distance <= $max_distance "
                "RETURN media, fn_distance, ln_distance "
                "ORDER BY fn_distance, ln_distance"
            )
            parameters = {"name": name, "max_distance": max_distance}
            return self.run_query(query, parameters)

# api/media/{id} PUT Update a media's personal details
    def update_media_properties(self, media_id, new_properties):
        query = (
            "MATCH (media:media) "
            "WHERE ID(media) = $media_id "
            "SET media += $new_properties "
            "RETURN media"
        )
        parameters = {"media_id": media_id, "new_properties": new_properties}
        return self.run_query(query, parameters)

# api/media/{id} PUT Update a media’s industries
    def add_media_industries(self, media_id, new_industry_list):
        query = f"MATCH (media) WHERE ID(media) = {media_id} SET media:{':'.join(new_industry_list)}"
        return self.run_query(query)

    def remove_media_industries(self, media_id, industries_to_remove):
        industries_to_remove_str = ":".join(industries_to_remove)
        query = f"MATCH (media) WHERE ID(media) = {media_id} REMOVE media:{industries_to_remove_str}"
        return self.run_query(query)

# api/media/{id}/history POST Add a new employment record for a media
# should include role, start date and end date
    def add_employment_record_for_media(self, media_id, company_id, relationship_properties):
        # Validate input IDs
        if not isinstance(media_id, int) or not isinstance(company_id, int):
            raise ValueError("Node IDs must be integers.")
        
        # Validate relationship properties
        if not isinstance(relationship_properties, dict):
            raise ValueError("Relationship properties must be a dictionary.")

        print("PLSSS")
        # Validate that the nodes are media and company nodes respectively
        is_media = neo4j_client.node_has_label(media_id, "media")
        print("HARLOOOO")
        print("is_media", is_media)
        is_company = neo4j_client.node_has_label(company_id, "company")

        if (is_media == False or is_company == False):
            raise ValueError("Nodes are the wrong type.")
        
        query = (
            "MATCH (media), (company) "
            "WHERE ID(media) = $media_id AND ID(company) = $company_id "
            "CREATE (media)-[r:%s]->(company) SET r = $relationship_properties" % "employed"
        )
        parameters = {
            "media_id": media_id,
            "company_id": company_id,
            "relationship_properties": relationship_properties
        }
        self.run_query(query, parameters)

# api/media/{id}/history GET Get all employment records for a media
    def get_all_employment_records(self, specific_node_id):
        query = (
            "MATCH (media)-[employment:EMPLOYMENT]->(company) "
            "WHERE ID(media) = $media_id "
            "RETURN employment, company"
        )
        parameters = {"media_id": media_id}
        result = self.run_query(query, parameters)
        return result

# api/media/{id}/notes GET Get all notes for a media
    def get_all_employment_records(self, media_id):
            query = (
                "MATCH (source_node)-[:CONNECTED_TO]->(target_node:%s) "
                "WHERE ID(source_node) = $media_id "
                "RETURN target_node" % "note"
            )
            parameters = {"media_id": media_id}
            result = self.run_query(query, parameters)
            return result

# api/media/{id}/notes POST Add a new note for a media
    def create_new_note_for_media(self, media_id, note_properties):
        query = (
            "MATCH (media) "
            "WHERE ID(media) = $media_id "
            "CREATE (note:note $note_properties) "
            "CREATE (media)-[:HAS_NOTE]->(note) "
            "RETURN note"
        )

        parameters = {
            "media_id": media_id,
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
            return result[0]['has_label']
        else:
            return False