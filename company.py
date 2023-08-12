from media.connection import Neo4jConnection
from datetime import datetime
from neo4j.time import Date
import uuid
import pycountry

class Company(Neo4jConnection):
    def __init__(self, uri, username, password):
        Neo4jConnection.__init__(self, uri, username, password)

    # api/companies/{id} GET Fetch a company based on ID
    def find_company_by_uid(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (company:Company) "
            "WHERE company.uid = $uid "
            "RETURN company"
        )
        return self.run_query(query, {"uid": uid})

    # api/companies GET Fetch companies based on name and specified industries
    def find_companies(self, json_request):
        # Extract data from the JSON
        name = json_request.get("name")
        max_distance = json_request.get("max_distance")
        industry_list = json_request.get("industry_list")

        # Construct the query
        query = (
            "MATCH (company:Company"
        )

        # If the industries are specified, add them to the query in the form of labels
        if industry_list:
            labels = ":".join(industry_list)
            query += f":{labels}"

        query += ") "

        # If a name is provided, conduct a fuzzy search
        if name:
            query += (
                "WITH company, "
                "     apoc.text.distance(toLower(company.company_name), toLower($name)) AS fn_distance, "
                "     apoc.text.distance(toLower(company.company_name), toLower($reversed_name)) AS reversed_fn_distance "
                "WHERE fn_distance <= $max_distance OR reversed_fn_distance <= $max_distance "
            )

        query += "RETURN company"
        
        parameters = {
            "name": name,
            "reversed_name": " ".join(reversed(name.split())) if name else "",
            "max_distance": max_distance
        }

        return self.run_query(query, parameters)

    # api/companies POST Add a new company to the database
    def add_company(self, json_request):
        # Generate a unique ID
        custom_id = str(uuid.uuid4())

        # Format the founding date str
        founded_date = datetime.strptime(json_request.get("founded_date"), "%Y-%m-%d").date()

        # Extract attributes from the JSON object
        parameters = {
            "uid": custom_id,
            "company_name": json_request.get("company_name"),
            "description": json_request.get("description"),
            "website_url": json_request.get("website_url"),
            "company_size_lower_bound": json_request.get("company_size_lower_bound"),
            "company_size_upper_bound": json_request.get("company_size_upper_bound"),
            "headquarters": json_request.get("headquarters"),
            "email": json_request.get("email"),
            "founded_date": founded_date
        }

        # Create a list of labels formatted as strings
        industries = json_request.get("industries", [])
        formatted_industries = [f":{industry}" for industry in industries]

        query = (
            "CREATE (company:Company {"
            "uid: $uid,"
            "company_name: $company_name,"
            "description: $description,"
            "website_url: $website_url,"
            "founded_date: date($founded_date),"
            "company_size_lower_bound: $company_size_lower_bound,"
            "company_size_upper_bound: $company_size_upper_bound,"
            "email: $email,"
            "headquarters: $headquarters"
            "})"
            "SET company" + "".join(formatted_industries) + " "  # Add labels using SET clause
            "RETURN company"
        )
        result = self.run_query(query, parameters)
        return result

    # api/companies/{id}/details PUT Update a company's details
    def update_company_properties(self, uid, new_properties):
        # Format the birthdate property as a Neo4j date
        if "founded_date" in new_properties:
            new_properties["founded_date"] = Date.from_iso_format(new_properties["founded_date"])

        # Construct query string
        query = (
            "MATCH (company:Company) "
            "WHERE company.uid = $uid "
            "SET company += $new_properties "
            "RETURN company"
        )
        parameters = {"uid": uid, "new_properties": new_properties}
        return self.run_query(query, parameters)

    #api/companies/{id}/industries PUT Update a company’s industries (labels)
    def update_company_industries(self, json_request):
        # Extract data from the JSON
        uid = json_request.get("uid")
        new_industry_list = json_request.get("new_industry_list")
        industries_to_remove = json_request.get("industries_to_remove")

        # Remove specified industries
        self.remove_company_industries(uid, industries_to_remove)

        # Add specified industries
        return self.add_company_industries(uid, new_industry_list)


    def add_company_industries(self, uid, new_industry_list):
        # Construct query string
        query = (
            "MATCH (company:Company) "
            "WHERE company.uid = $uid "
            "SET company:"
            + ":".join(new_industry_list)
            + " "
            "RETURN company"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    def remove_company_industries(self, uid, industries_to_remove):
        # Format the list of labels to remove
        industries_to_remove_str = ":".join(industries_to_remove)

        # Construct query string
        query = (
            "MATCH (company:Company) "
            "WHERE company.uid = $uid "
            "REMOVE company:"
            + industries_to_remove_str
            + " "
            "RETURN company"
        )
        parameters = {"uid": uid}
        return self.run_query(query, parameters)

    # api/companies/{id}/employees GET Get all current and old employees from a company
    def get_all_employment_records(self, json_request):
        # Extract fields from JSON
        uid = json_request.get("uid")

        # Construct query string
        query = (
            "MATCH (employee)-[employment:EMPLOYMENT]->(company) "
            "WHERE company.uid = $uid "
            "RETURN employment, employee, company"
        )

        parameters = {"uid": uid}
        result = self.run_query(query, parameters)
        return result