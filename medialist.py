from media.connection import Neo4jConnection
from datetime import datetime, date
from neo4j.time import Date
import uuid

class Medialist(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/medialists GET Fetch companies based on name and specified industries
    def find_medialists(self, json_request):
        # Extract data from the JSON
        name = json_request.get("name")
        max_distance = json_request.get("max_distance")
        industry_list = json_request.get("industry_list")

        # Construct the query
        query = (
            "MATCH (medialist:Medialist"
        )

        # If the industries are specified, add them to the query in the form of labels
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"

        query += ") "

        # If a name is provided, conduct a fuzzy search
        if name:
            query += (
                "WITH medialist, "
                "     apoc.text.distance(toLower(medialist.medialist_name), toLower($name)) AS fn_distance, "
                "     apoc.text.distance(toLower(medialist.medialist_name), toLower($reversed_name)) AS reversed_fn_distance "
                "WHERE fn_distance <= $max_distance OR reversed_fn_distance <= $max_distance "
            )

        query += "RETURN medialist"
        
        parameters = {
            "name": name,
            "reversed_name": " ".join(reversed(name.split())) if name else "",
            "max_distance": max_distance
        }
        return self.run_query(query, parameters)

    # api/medialists POST Add a new media list to the database
    def add_medialist(self, json_request):
        # Generate a unique ID
        custom_id = str(uuid.uuid4())

        # Extract fields from the JSON
        medialist_name = json_request.get("medialist_name")
        description = json_request.get("description")
        creation_datetime = datetime.now()

        # Create a list of labels formatted as strings
        industries = json_request.get("industries", [])
        formatted_industries = [f":{industry}" for industry in industries]

        query = (
            "CREATE (medialist:Medialist {"
            "uid: $uid,"
            "medialist_name: $medialist_name,"
            "creation_datetime: date($creation_datetime),"
            "description: $description"
            "})"
            "SET medialist" + "".join(formatted_industries) + " "  # Add labels using SET clause
            "RETURN medialist"
        )
        parameters = {
            "uid": custom_id,
            "medialist_name": medialist_name,
            "creation_datetime": datetime.now(),
            "description": description
        }
        result = self.run_query(query, parameters)
        return result

    # api/medialists/{id} GET Fetch a medialist based on ID
    def find_medialist_by_uid(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (medialist:Medialist) "
            "WHERE medialist.uid = $uid "
            "RETURN medialist"
        )
        return self.run_query(query, {"uid": uid})

    # api/medialists/{id} PUT Update a journalist's personal details
    def update_medialist_industries(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        new_industry_list = json_request.get("new_industry_list")
        industries_to_remove = json_request.get("industries_to_remove")

        # Remove specified industries
        self.remove_medialist_industries(uid, industries_to_remove)

        # Add specified industries
        return self.add_medialist_industries(uid, new_industry_list)

    def add_medialist_industries(self, uid, new_industry_list):
        # Construct query string
        query = (
            "MATCH (medialist:Medialist) "
            "WHERE medialist.uid = $uid "
            "SET medialist:"
            + ":".join(new_industry_list)
            + " "
            "RETURN medialist"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    def remove_medialist_industries(self, uid, industries_to_remove):
        # Format the list of labels to remove
        industries_to_remove_str = ":".join(industries_to_remove)

        # Construct query string
        query = (
            "MATCH (medialist:Medialist) "
            "WHERE medialist.uid = $uid "
            "REMOVE medialist:"
            + industries_to_remove_str
            + " "
            "RETURN medialist"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    # api/medialists/{id}/details PUT Update a medialist’s properties
    def update_medialist_properties(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        new_properties = json_request.get("new_properties")

        # Construct query string
        query = (
            "MATCH (medialist:Medialist) "
            "WHERE medialist.uid = $uid "
            "SET medialist += $new_properties "
            "RETURN medialist"
        )
        parameters = {"uid": uid, "new_properties": new_properties}
        return self.run_query(query, parameters)

    # api/medialists/{id} POST Add a new person to the media list
    def add_to_medialist(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        medialist_uid = json_request.get("medialist_uid")
        new_properties = json_request.get("relationship_properties")
        new_properties["creation_datetime"] = datetime.now()

        # Validate relationship properties
        if not isinstance(new_properties, dict):
            raise ValueError("Relationship properties must be a dictionary.")

        # Construct query string
        query = (
            "MATCH (person) "
            "WHERE person.uid = $uid "
            "MATCH (medialist:Medialist) WHERE medialist.uid = $medialist_uid "
            "CREATE (person)-[r:INCLUDED]->(medialist) SET r = $new_properties "
            "RETURN r, person, medialist"
        )
        parameters = {
            "uid": uid,
            "medialist_uid": medialist_uid,
            "new_properties": new_properties
        }
        return self.run_query(query, parameters)

    # api/medialists/{id}/all GET Get all people in a media list
    def get_all_in_medialist(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (person)-[included:INCLUDED]->(medialist) "
            "WHERE medialist.uid = $uid "
            "RETURN included, person, medialist"
        )
        parameters = {"uid": uid}
        result = self.run_query(query, parameters)
        return result