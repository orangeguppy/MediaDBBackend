from media.connection import Neo4jConnection
from datetime import datetime, date
from neo4j.time import Date
import uuid

class Journalist(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/journalists GET Fetch companies based on name and specified industries
    def find_journalists(self, json_request):
        # Extract data from the JSON
        name = json_request.get("name")
        max_distance = json_request.get("max_distance")
        industry_list = json_request.get("industry_list")

        # Construct the query
        query = (
            "MATCH (journalist:Journalist"
        )

        # If the industries are specified, add them to the query in the form of labels
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"

        query += ") "

        # If a name is provided, conduct a fuzzy search
        if name:
            query = (
            "MATCH (journalist:Journalist) "
            "WITH journalist, "
            "     apoc.text.distance(toLower(journalist.first_name), toLower($name)) AS fn_distance, "
            "     apoc.text.distance(toLower(journalist.last_name), toLower($name)) AS ln_distance, "
            "     apoc.text.distance(toLower(journalist.first_name), toLower($reversed_name)) AS reversed_fn_distance, "
            "     apoc.text.distance(toLower(journalist.last_name), toLower($reversed_name)) AS reversed_ln_distance "
            "WHERE fn_distance <= $max_distance OR ln_distance <= $max_distance "
            )

        query += "RETURN journalist"
        
        parameters = {
            "name": name,
            "reversed_name": " ".join(reversed(name.split())) if name else "",
            "max_distance": max_distance
        }
        return self.run_query(query, parameters)

    # api/journalists POST Add a new journalist to the database
    def add_journalist(self, json_request):
        # Generate a unique ID
        custom_id = str(uuid.uuid4())

        # Format the birthdate str
        birthdate = datetime.strptime(json_request.get("birthdate"), "%Y-%m-%d").date()

        # Extract attributes from the JSON object
        parameters = {
            "uid": custom_id,
            "first_name": json_request.get("first_name"),
            "last_name": json_request.get("last_name"),
            "birthdate": birthdate,
            "description": json_request.get("description"),
            "email": json_request.get("email"),
            "mobile_num": json_request.get("mobile_num")
        }

        # Create a list of labels formatted as strings
        industries = json_request.get("industries", [])
        formatted_industries = [f":{industry}" for industry in industries]

        query = (
            "CREATE (journalist:Journalist {"
            "uid: $uid,"
            "first_name: $first_name,"
            "last_name: $last_name,"
            "birthdate: date($birthdate),"
            "description: $description,"
            "email: $email,"
            "mobile_num: $mobile_num"
            "})"
            "SET journalist" + "".join(formatted_industries) + " "  # Add labels using SET clause
            "RETURN journalist"
        )
        result = self.run_query(query, parameters)
        return result

    # api/journalists/{id} GET Fetch a journalist based on ID
    def find_journalist_by_uid(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "RETURN journalist"
        )
        return self.run_query(query, {"uid": uid})

    # api/journalists/{id} PUT Update a journalist's personal details
    def update_journalist_industries(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        new_industry_list = json_request.get("new_industry_list")
        industries_to_remove = json_request.get("industries_to_remove")

        # Remove specified industries
        self.remove_journalist_industries(uid, industries_to_remove)

        # Add specified industries
        return self.add_journalist_industries(uid, new_industry_list)

    def add_journalist_industries(self, uid, new_industry_list):
        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "SET journalist:"
            + ":".join(new_industry_list)
            + " "
            "RETURN journalist"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    def remove_journalist_industries(self, uid, industries_to_remove):
        # Format the list of labels to remove
        industries_to_remove_str = ":".join(industries_to_remove)

        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "REMOVE journalist:"
            + industries_to_remove_str
            + " "
            "RETURN journalist"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    def update_journalist_properties(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        new_properties = json_request.get("new_properties")

        # Format the birthdate property as a Neo4j date
        if "birthdate" in new_properties:
            new_properties["birthdate"] = Date.from_iso_format(new_properties["birthdate"])

        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "SET journalist += $new_properties "
            "RETURN journalist"
        )
        parameters = {"uid": uid, "new_properties": new_properties}
        return self.run_query(query, parameters)

    # api/journalists/{id}/history POST Add a new employment record for a journalist
    # should include role, start date and end date
    def add_employment_record(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        company_uid = json_request.get("company_uid")
        new_properties = json_request.get("relationship_properties")
        
        # Format the start and end date properties as a Neo4j date
        if "start_date" in new_properties:
            new_properties["start_date"] = Date.from_iso_format(new_properties["start_date"])
        if "end_date" in new_properties:
            new_properties["end_date"] = Date.from_iso_format(new_properties["end_date"])

        # Validate relationship properties
        if not isinstance(new_properties, dict):
            raise ValueError("Relationship properties must be a dictionary.")

        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "MATCH (company:Company) WHERE company.uid = $company_uid "
            "CREATE (journalist)-[r:EMPLOYMENT]->(company) SET r = $new_properties "
            "RETURN r, journalist, company"
        )
        parameters = {
            "uid": uid,
            "company_uid": company_uid,
            "new_properties": new_properties
        }
        return self.run_query(query, parameters)

    # api/journalists/{id}/history GET Get all employment records for a journalist
    def get_all_employment_records(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (journalist:Journalist)-[employment:EMPLOYMENT]->(company) "
            "WHERE journalist.uid = $uid "
            "RETURN employment, journalist, company"
        )
        parameters = {"uid": uid}
        result = self.run_query(query, parameters)
        return result

    # api/journalists/{id}/notes GET Get all notes for a journalist
    def get_all_notes(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (author)-[note:NOTE]->(journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "RETURN note, journalist, author"
        )
        parameters = {"uid": uid}
        result = self.run_query(query, parameters)
        return result

    # api/journalists/{id}/notes POST Add a new note for a journalist
    def create_new_note_for_journalist(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")
        author_uid = json_request.get("author_uid")
        creation_date = date.today()
        content = json_request.get("content")

        # Construct query string
        query = (
            "MATCH (journalist:Journalist) "
            "WHERE journalist.uid = $uid "
            "MATCH (author) WHERE author.uid = $author_uid "
            "CREATE (author)-[r:NOTE]->(journalist) SET r = $note_properties "
            "RETURN r, journalist, author"
        )

        parameters = {
            "uid": uid,
            "author_uid": author_uid,
            "note_properties": {
                "creation_date": creation_date,
                "content": content
            }
        }
        result = self.run_query(query, parameters)
        return result

# Check if a node has a specific label
    def node_has_label(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")
        label = json_request.get("label")

        # Construct query string
        query = (
            "MATCH (node) "
            "WHERE node.uid = $uid AND $label IN labels(node) "
            "RETURN COUNT(node) > 0 AS has_label"
        )
        parameters = {"node_id": node_id, "label": label}
        result = self.run_query(query, parameters)

        if result:
            return True
        else:
            return False