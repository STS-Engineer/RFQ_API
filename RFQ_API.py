import os
import base64
import time
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2 import OperationalError, errorcodes, extras
import requests
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from pydantic import BaseModel, Field, ValidationError

# ------------------------ .env setup ------------------------
load_dotenv()
ENV_API_KEY = os.getenv("APOLLO_API_KEY")
ALLOW_HEADER_OVERRIDE = os.getenv("ALLOW_HEADER_OVERRIDE", "false").lower() in ("1", "true", "yes")

# --- 1. CONFIGURATION ---
# PostgreSQL configuration
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}

app = Flask(__name__)

# ------------------------ Pydantic Models ------------------------
class SearchPeopleRequest(BaseModel):
    q_organization_name: List[str] = Field(..., description="List of company names to search")
    person_titles: Optional[List[str]] = Field(None, description="Array of job titles to filter by")
    person_seniorities: Optional[List[str]] = Field(None, description="Filter by seniority levels")
    organization_num_employees_ranges: Optional[List[str]] = Field(None, description="Filter by company size")
    q_organization_domains: Optional[List[str]] = Field(None, description="Filter by company domain")
    page: int = Field(1, ge=1, le=500, description="Page number for pagination")
    per_page: int = Field(25, ge=1, le=100, description="Number of results per page per organization")
    delay_between_requests: float = Field(1.0, ge=0, description="Delay in seconds between API calls")

class EnrichPersonRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    name: Optional[str] = None
    organization_name: Optional[str] = None
    domain: Optional[str] = None
    email: Optional[str] = None
    id: Optional[str] = None
    linkedin_url: Optional[str] = None
    reveal_personal_emails: bool = False
    reveal_phone_number: bool = False
    webhook_url: Optional[str] = None

class BulkEnrichRequest(BaseModel):
    details: List[Dict[str, Any]] = Field(..., max_items=10)
    reveal_personal_emails: bool = False
    reveal_phone_number: bool = False
    webhook_url: Optional[str] = None

# ------------------------ Apollo Client ------------------------
class ApolloClient:
    """Client for Apollo.io API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.apollo.io/api/v1"
        self.headers = {
            "X-Api-Key": api_key,
            "Content-Type": "application/json"
        }

    def search_single_organization(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/mixed_people/search",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    def enrich_person(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/people/match",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    def bulk_enrich(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            f"{self.base_url}/people/bulk_match",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

# ------------------------ Helpers ------------------------
def get_apollo_client(api_key: Optional[str] = None) -> ApolloClient:
    """
    Use provided key if ALLOW_HEADER_OVERRIDE=true, otherwise use .env key.
    """
    if ALLOW_HEADER_OVERRIDE and api_key:
        return ApolloClient(api_key)
    if not ENV_API_KEY:
        raise RuntimeError("APOLLO_API_KEY is not set. Add it to your .env or environment variables.")
    return ApolloClient(ENV_API_KEY)

# --- 2. DATABASE CONNECTION UTILITY ---
def get_db():
    """Returns a PostgreSQL database connection and a RealDictCursor."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        return conn, cursor
    except OperationalError as e:
        print(f"PostgreSQL connection failed: {e}")
        if conn:
            conn.close()
        error_message = f"Database connection failed: {str(e)}"
        raise ConnectionError(error_message)
    except Exception as e:
        if conn:
            conn.close()
        raise e

def convert_to_boolean(value):
    """Safely converts string/bool input to a Python boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['yes', 'true']
    return False

# ------------------------ Apollo Routes ------------------------
@app.route('/')
def root():
    return {
        "status": "online",
        "service": "Merged RFQ & Apollo.io API",
        "version": "1.0.0",
        "endpoints": {
            "rfq_submit": "/api/rfq/submit",
            "rfq_get": "/api/rfq/get",
            "products": "/api/products",
            "product_lines": "/api/product-lines",
            "product_lines_list": "/api/product-lines/list",
            "product_lines_details": "/api/product-lines/details",
            "apollo_search": "/apollo/search",
            "apollo_enrich": "/apollo/enrich",
            "apollo_bulk_enrich": "/apollo/bulk_enrich"
        }
    }

@app.route('/health')
def health():
    return {"status": "healthy"}

@app.route('/apollo/search', methods=['POST'])
@app.route('/api/v1/mixed_people/search', methods=['POST'])
def search_people_simple():
    """
    Search for people across multiple organizations.
    Loops through each organization and aggregates results.
    """
    data = request.get_json()
    x_api_key = request.headers.get('X-Api-Key')
    
    try:
        req = SearchPeopleRequest(**data)
    except ValidationError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    try:
        client = get_apollo_client(x_api_key)
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    all_contacts: List[Dict[str, Any]] = []
    total_entries = 0
    organizations_searched: List[str] = []
    errors: List[Dict[str, str]] = []

    for idx, org_name in enumerate(req.q_organization_name):
        try:
            payload: Dict[str, Any] = {
                "q_organization_name": org_name,
                "page": req.page,
                "per_page": req.per_page
            }
            if req.person_titles:
                payload["person_titles"] = req.person_titles
            if req.person_seniorities:
                payload["person_seniorities"] = req.person_seniorities
            if req.organization_num_employees_ranges:
                payload["organization_num_employees_ranges"] = req.organization_num_employees_ranges
            if req.q_organization_domains:
                payload["q_organization_domains"] = req.q_organization_domains

            data = client.search_single_organization(payload)

            contacts = data.get("contacts", []) or data.get("people", [])
            all_contacts.extend(contacts)

            pagination = data.get("pagination", {})
            total_entries += pagination.get("total_entries", 0)
            organizations_searched.append(org_name)

            print(f"✓ Found {len(contacts)} contacts from {org_name}")

            if idx < len(req.q_organization_name) - 1:
                time.sleep(req.delay_between_requests)

        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", "unknown")
            errors.append({"organization": org_name, "error": f"HTTP error searching {org_name}: {status}"})
            print(f"✗ HTTP error searching {org_name}: {status}")
        except Exception as e:
            errors.append({"organization": org_name, "error": f"Error searching {org_name}: {str(e)}"})
            print(f"✗ Error searching {org_name}: {str(e)}")

    response: Dict[str, Any] = {
        "organizations_searched": organizations_searched,
        "total_organizations": len(req.q_organization_name),
        "successful_searches": len(organizations_searched),
        "failed_searches": len(errors),
        "total_contacts": len(all_contacts),
        "contacts": all_contacts,
        "pagination": {
            "page": req.page,
            "per_page": req.per_page,
            "total_entries": total_entries
        }
    }
    if errors:
        response["errors"] = errors
    
    return jsonify(response), 200

@app.route('/apollo/enrich', methods=['POST'])
@app.route('/api/v1/people/match', methods=['POST'])
def enrich_person():
    data = request.get_json()
    x_api_key = request.headers.get('X-Api-Key')
    
    try:
        req = EnrichPersonRequest(**data)
    except ValidationError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    # Validation
    if req.reveal_phone_number and not req.webhook_url:
        return jsonify({
            "status": "error",
            "message": "webhook_url is mandatory when reveal_phone_number is True"
        }), 400

    payload: Dict[str, Any] = {}
    if req.name:
        payload["name"] = req.name
    else:
        if req.first_name:
            payload["first_name"] = req.first_name
        if req.last_name:
            payload["last_name"] = req.last_name

    if req.organization_name:
        payload["organization_name"] = req.organization_name
    if req.domain:
        payload["domain"] = req.domain
    if req.email:
        payload["email"] = req.email
    if req.id:
        payload["id"] = req.id
    if req.linkedin_url:
        payload["linkedin_url"] = req.linkedin_url
    if req.reveal_personal_emails:
        payload["reveal_personal_emails"] = True
    if req.reveal_phone_number:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = req.webhook_url

    try:
        client = get_apollo_client(x_api_key)
        data = client.enrich_person(payload)
        return jsonify(data), 200
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"status": "error", "message": text}), status
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/apollo/bulk_enrich', methods=['POST'])
@app.route('/api/v1/people/bulk_match', methods=['POST'])
def bulk_enrich_people():
    data = request.get_json()
    x_api_key = request.headers.get('X-Api-Key')
    
    try:
        req = BulkEnrichRequest(**data)
    except ValidationError as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    if req.reveal_phone_number and not req.webhook_url:
        return jsonify({
            "status": "error",
            "message": "webhook_url is mandatory when reveal_phone_number is True"
        }), 400

    payload: Dict[str, Any] = {"details": req.details}
    if req.reveal_personal_emails:
        payload["reveal_personal_emails"] = True
    if req.reveal_phone_number:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = req.webhook_url

    try:
        client = get_apollo_client(x_api_key)
        data = client.bulk_enrich(payload)
        return jsonify(data), 200
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"status": "error", "message": text}), status
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- 3. RFQ API ENDPOINTS ---
@app.route('/api/rfq/submit', methods=['POST'])
def submit_rfq_data():
    """Receives structured RFQ data and inserts it into the main and contact tables."""
    data = request.get_json()
    conn = None
    rfq_id = None
    
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    rfq_id = data.get('rfq_id')
    if not rfq_id:
        return jsonify({"status": "error", "message": "Missing required field: rfq_id"}), 400

    try:
        conn, cursor = get_db()
        
        contact_data = data.get('contact', {})
        contact_email = contact_data.get('email')
        
        if not contact_email:
            conn.close()
            return jsonify({"status": "error", "message": "Missing required contact field: email"}), 400

        contact_id_fk = None

        cursor.execute(
            "SELECT contact_id FROM contact WHERE contact_email = %s", 
            (contact_email,)
        )
        existing_contact = cursor.fetchone()

        if existing_contact:
            contact_id_fk = existing_contact['contact_id']
        else:
            insert_contact_sql = """
            INSERT INTO contact (contact_role, contact_email, contact_phone)
            VALUES (%s, %s, %s)
            RETURNING contact_id;
            """
            cursor.execute(
                insert_contact_sql,
                (
                    contact_data.get('role'),
                    contact_data.get('email'),
                    contact_data.get('phone')
                )
            )
            contact_id_fk = cursor.fetchone()['contact_id']

        main_fields = {
            'rfq_id': rfq_id,
            'customer_name': data.get('customer_name'),
            'application': data.get('application'),
            'product_line': data.get('product_line'),
            'customer_pn': data.get('customer_pn'),
            'revision_level': data.get('revision_level'),
            'delivery_zone': data.get('delivery_zone'),
            'delivery_plant': data.get('delivery_plant'),
            'sop_year': data.get('sop_year'),
            'annual_volume': data.get('annual_volume'),
            'rfq_reception_date': data.get('rfq_reception_date'),
            'quotation_expected_date': data.get('quotation_expected_date'),
            'target_price_eur': data.get('target_price_eur'),
            'delivery_conditions': data.get('delivery_conditions'),
            'payment_terms': data.get('payment_terms'),
            'business_trigger': data.get('business_trigger'),
            'entry_barriers': data.get('entry_barriers'),
            'product_feasibility_note': data.get('product_feasibility_note'),
            'manufacturing_location': data.get('manufacturing_location'),
            'risks': data.get('risks'),
            'decision': data.get('decision'),
            'design_responsibility': data.get('design_responsibility'),
            'validation_responsibility': data.get('validation_responsibility'),
            'design_ownership': data.get('design_ownership'),
            'development_costs': data.get('development_costs'),
            'technical_capacity': convert_to_boolean(data.get('technical_capacity', 'maybe')),
            'scope_alignment': convert_to_boolean(data.get('scope_alignment', 'maybe')),
            'overall_feasibility': data.get('overall_feasibility'),
            'customer_status': data.get('customer_status'),
            'strategic_note': data.get('strategic_note'),
            'final_recommendation': data.get('final_recommendation'),
            'contact_id_fk': contact_id_fk
        }
        
        main_columns = ', '.join(main_fields.keys())
        main_placeholders = ', '.join(['%s'] * len(main_fields))
        main_values = list(main_fields.values())

        insert_main_sql = f"""
        INSERT INTO main ({main_columns})
        VALUES ({main_placeholders})
        """
        
        cursor.execute(insert_main_sql, main_values)
        conn.commit()

        return jsonify({
            "status": "success", 
            "message": "RFQ data successfully stored.", 
            "rfq_id": rfq_id
        }), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    except OperationalError as e:
        if conn:
            conn.rollback()
            
        pg_error_message = getattr(e, 'pgerror', None)
        detail_message = pg_error_message.strip() if pg_error_message else str(e)

        return jsonify({
            "status": "error",
            "message": f"Database insertion failed (PostgreSQL Error): {detail_message}",
            "rfq_id": rfq_id if rfq_id else None
        }), 500

    except Exception as e:
        if conn:
            conn.rollback()
            
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/rfq/get', methods=['GET'])
def get_rfq_data():
    """Retrieves RFQ data based on dynamic query parameters (rfq_id, customer_name, product_line)."""
    
    rfq_id = request.args.get('rfq_id')
    customer_name = request.args.get('customer_name')
    product_line = request.args.get('product_line')
    
    where_clauses = []
    query_params = []
    
    if rfq_id:
        where_clauses.append("m.rfq_id = %s")
        query_params.append(rfq_id)

    if customer_name:
        where_clauses.append("m.customer_name ILIKE %s")
        query_params.append(f"%{customer_name}%")

    if product_line:
        where_clauses.append("m.product_line = %s")
        query_params.append(product_line)

    if not where_clauses:
        return jsonify({"status": "error", "message": "At least one search parameter (rfq_id, customer_name, or product_line) is required."}), 400

    where_sql = " AND ".join(where_clauses)
    
    conn = None
    try:
        conn, cursor = get_db()
        
        select_sql = f"""
        SELECT 
            m.*, 
            c.contact_role, 
            c.contact_email, 
            c.contact_phone
        FROM 
            main m
        INNER JOIN 
            contact c ON m.contact_id_fk = c.contact_id
        WHERE 
            {where_sql}
        ORDER BY 
            m.rfq_reception_date DESC;
        """
        
        cursor.execute(select_sql, tuple(query_params))
        
        results = cursor.fetchall()
        
        if not results:
            return jsonify({
                "status": "success",
                "message": "No RFQ records found matching the criteria.",
                "data": []
            }), 200

        formatted_results = []
        for row in results:
            rfq_data = dict(row)
            
            contact = {
                'role': rfq_data.pop('contact_role', None),
                'email': rfq_data.pop('contact_email', None),
                'phone': rfq_data.pop('contact_phone', None)
            }
            rfq_data.pop('contact_id_fk', None) 
            rfq_data.pop('contact_id', None)
            
            rfq_data['contact'] = contact
            formatted_results.append(rfq_data)

        return jsonify({
            "status": "success", 
            "message": f"Retrieved {len(formatted_results)} RFQ record(s).",
            "data": formatted_results
        }), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    except OperationalError as e:
        print(f"PostgreSQL query failed: {e}")
        return jsonify({
            "status": "error",
            "message": f"Database query failed: {str(e)}",
            "query_params": request.args
        }), 500

    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if conn:
            conn.close()

@app.route('/api/products', methods=['GET'])
def retrieve_products_modified():
    """
    Retrieves product data from the 'products' table, excluding the large 'product_pictures' column.
    """
    product_name = request.args.get('productName')

    conn = None
    try:
        conn, cursor = get_db()
        
        select_columns = """
            id, product_name, product_line, description, product_definition, 
            operating_environment, technical_parameters, machines_and_tooling, 
            manufacturing_strategy, purchasing_strategy, prototypes_ppap_and_sop, 
            engineering_and_testing, capacity, our_advantages, gmdc_pct, 
            product_line_id, customers_in_production, customer_in_development, 
            level_of_interest_and_why, estimated_price_per_product, 
            prod_if_customer_in_china, costing_data, created_at
        """
        
        query = f"SELECT {select_columns} FROM public.products"
        search_pattern = None
        
        if product_name:
            query += " WHERE product_name ILIKE %s"
            search_pattern = f"%{product_name}%"
            cursor.execute(query, (search_pattern,))
        else:
            cursor.execute(query) 

        products_data = cursor.fetchall()

        return jsonify({
            "query": product_name if product_name else "All Products",
            "products": products_data, 
            "source": "database"     
        }), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "error": "Error retrieving products from the database",
            "details": pg_error_message
        }), 400
        
    finally:
        if conn:
            conn.close()

@app.route('/api/product-lines', methods=['GET'])
def retrieve_product_line_modified():
    """
    Retrieves product line data from the 'product_lines' table using a required ID.
    """
    product_line_id = request.args.get('productLineId') 

    if not product_line_id:
        return jsonify({
            "error": "Missing required query parameter: productLineId",
            "details": "The ID of the product line must be provided to retrieve its details."
        }), 400 

    conn = None
    try:
        conn, cursor = get_db()

        query = """
            SELECT *
            FROM public.product_lines
            WHERE id = %s;
        """
        cursor.execute(query, (product_line_id,))

        product_line_data = cursor.fetchall()

        return jsonify({
            "query": product_line_id,
            "productLine": product_line_data, 
            "source": "database"
        }), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "error": "Error retrieving product-line items from the database",
            "details": pg_error_message
        }), 400

    finally:
        if conn:
            conn.close()

@app.route('/api/product-lines/list', methods=['GET'])
def list_product_lines():
    """
    Retrieves the list of all product lines (ID and Name) for selection menus.
    """
    conn = None
    try:
        conn, cursor = get_db()

        query = """
            SELECT id, name AS product_line_name, type_of_products AS description_snippet
            FROM public.product_lines
            ORDER BY id;
        """
        cursor.execute(query)

        product_lines_list = cursor.fetchall()

        return jsonify({
            "productLinesList": product_lines_list,
            "count": len(product_lines_list),
            "source": "database"
        }), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "error": "Error retrieving product lines list from the database",
            "details": pg_error_message
        }), 400

    finally:
        if conn:
            conn.close()

@app.route('/api/product-lines/details', methods=['GET'])
def get_product_line_by_product_name():
    """
    Retrieves product line details based on a given product name.
    """
    product_name = request.args.get('productName')

    if not product_name:
        return jsonify({
            "error": "Missing required query parameter: productName",
            "details": "Provide a valid product name to retrieve product-line details."
        }), 400

    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        query = """
            SELECT 
                pl.id AS product_line_id,
                pl.name AS product_line_name,
                pl.type_of_products,
                pl.manufacturing_locations,
                pl.design_center,
                pl.product_line_manager,
                pl.type_of_customers,
                pl.metiers,
                pl.strength,
                pl.weakness,
                pl.perspectives,
                pl.history,
                p.id AS product_id,
                p.product_name,
                p.description AS product_description,
                p.product_definition,
                p.operating_environment,
                p.technical_parameters
            FROM public.products p
            INNER JOIN public.product_lines pl 
                ON p.product_line_id = pl.id
            WHERE p.product_name ILIKE %s;
        """

        cursor.execute(query, (f"%{product_name}%",))
        results = cursor.fetchall()

        if not results:
            return jsonify({
                "status": "success",
                "message": "No product line found matching the provided product name.",
                "data": []
            }), 200

        return jsonify({
            "status": "success",
            "message": f"Retrieved {len(results)} product-line record(s) for '{product_name}'.",
            "data": results
        }), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "error": "Error retrieving product-line details from the database",
            "details": pg_error_message
        }), 500

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(error_details)
        return jsonify({
            "error": "An unexpected error occurred",
            "details": str(e)
        }), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True)