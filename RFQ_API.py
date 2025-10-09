import os
import base64
import time
from typing import List, Dict, Any, Optional, Tuple
import psycopg2
from psycopg2 import OperationalError, errorcodes, extras
from flask import Flask, request, jsonify
import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

# --- APOLLO API SETUP ---
load_dotenv()
ENV_API_KEY = os.getenv("APOLLO_API_KEY")
ALLOW_HEADER_OVERRIDE = os.getenv("ALLOW_HEADER_OVERRIDE", "false").lower() in ("1", "true", "yes")

if not ENV_API_KEY:
    raise RuntimeError("APOLLO_API_KEY is not set. Add it to your .env or environment variables.")

# --- 1. CONFIGURATION ---
# IMPORTANT: Replace these placeholders with your actual PostgreSQL credentials.
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}

app = Flask(__name__)

# --- APOLLO PYDANTIC MODELS ---
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

# --- 2. DATABASE CONNECTION UTILITY ---

def get_db():
    """Returns a PostgreSQL database connection and a RealDictCursor."""
    conn = None
    try:
        # Establish connection using the configuration dictionary
        conn = psycopg2.connect(**DB_CONFIG)
        # Set the row factory to return dictionaries
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        return conn, cursor
    except OperationalError as e:
        # Log the specific connection failure
        print(f"PostgreSQL connection failed: {e}")
        # Re-raise the error to be handled by the API endpoint caller
        if conn:
            conn.close()
        # Use str(e) as a fallback if e.pgerror is not available (common in connection errors)
        error_message = f"Database connection failed: {str(e)}"
        raise ConnectionError(error_message)
    except Exception as e:
        if conn:
            conn.close()
        raise e


def convert_to_boolean(value):
    """Safely converts string/bool input to a Python boolean."""
    if isinstance(value, bool):
        return value # Handles true/false from JSON
    if isinstance(value, str):
        # Handles "yes"/"true" strings
        return value.lower() in ['yes', 'true']
    # Default behavior for missing/unexpected values (e.g., None, 'maybe')
    return False 

# --- APOLLO CLIENT CLASS ---
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

# --- APOLLO HELPERS ---
def get_apollo_client_from_request(req) -> ApolloClient:
    """
    Use header key only if ALLOW_HEADER_OVERRIDE=true and header provided.
    Otherwise, use the .env key.
    """
    header_key = req.headers.get("X-Api-Key")
    if ALLOW_HEADER_OVERRIDE and header_key:
        return ApolloClient(header_key)
    return ApolloClient(ENV_API_KEY)

def parse_body(model_cls) -> Tuple[Optional[BaseModel], Optional[Dict[str, Any]]]:
    """
    Parse and validate JSON body with Pydantic. Returns (model_instance, error_json).
    """
    try:
        payload = request.get_json(silent=True) or {}
        obj = model_cls(**payload)
        return obj, None
    except ValidationError as ve:
        return None, {"detail": ve.errors()}
    except Exception as e:
        return None, {"detail": str(e)}

# --- 3. API ENDPOINTS ---

# Root endpoint
@app.get("/")
def root():
    return jsonify({
        "status": "online",
        "service": "RFQ API with Apollo.io Integration",
        "version": "1.0.0",
        "endpoints": {
            "rfq": {
                "submit": "/api/rfq/submit",
                "get": "/api/rfq/get"
            },
            "products": {
                "list": "/api/products",
                "lines": "/api/product-lines",
                "lines_list": "/api/product-lines/list",
                "lines_details": "/api/product-lines/details"
            },
            "apollo": {
                "search": "/apollo/search",
                "enrich": "/apollo/enrich",
                "bulk_enrich": "/apollo/bulk_enrich"
            }
        }
    })

@app.get("/health")
def health():
    return jsonify({"status": "healthy"})

# --- APOLLO ENDPOINTS ---

@app.post("/apollo/search")
def search_people_simple():
    """
    Search for people across multiple organizations.
    Loops through each organization and aggregates results.
    """
    req, err = parse_body(SearchPeopleRequest)
    if err:
        return jsonify(err), 422

    client = get_apollo_client_from_request(request)

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
    return jsonify(response), (207 if errors else 200)

# Mirror FastAPI alias
@app.post("/api/v1/mixed_people/search")
def search_people_alias():
    return search_people_simple()

@app.post("/apollo/enrich")
@app.post("/api/v1/people/match")
def enrich_person():
    req, err = parse_body(EnrichPersonRequest)
    if err:
        return jsonify(err), 422

    # Validation parity with FastAPI version
    if req.reveal_phone_number and not req.webhook_url:
        return jsonify({
            "detail": "webhook_url is mandatory when reveal_phone_number is True"
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

    client = get_apollo_client_from_request(request)
    try:
        data = client.enrich_person(payload)
        return jsonify(data)
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"detail": text}), status
    except Exception as e:
        return jsonify({"detail": str(e)}), 500

@app.post("/apollo/bulk_enrich")
@app.post("/api/v1/people/bulk_match")
def bulk_enrich_people():
    req, err = parse_body(BulkEnrichRequest)
    if err:
        return jsonify(err), 422

    if req.reveal_phone_number and not req.webhook_url:
        return jsonify({
            "detail": "webhook_url is mandatory when reveal_phone_number is True"
        }), 400

    payload: Dict[str, Any] = {"details": req.details}
    if req.reveal_personal_emails:
        payload["reveal_personal_emails"] = True
    if req.reveal_phone_number:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = req.webhook_url

    client = get_apollo_client_from_request(request)
    try:
        data = client.bulk_enrich(payload)
        return jsonify(data)
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"detail": text}), status
    except Exception as e:
        return jsonify({"detail": str(e)}), 500

# --- RFQ ENDPOINTS ---

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
        # 1. Establish connection
        conn, cursor = get_db()
        
        # Start transaction block
        
        # --- CONTACT HANDLING (1:N Relationship) ---
        contact_data = data.get('contact', {})
        contact_email = contact_data.get('email')
        
        if not contact_email:
            conn.close()
            return jsonify({"status": "error", "message": "Missing required contact field: email"}), 400

        contact_id_fk = None

        # A. Try to find existing contact by email
        cursor.execute(
            "SELECT contact_id FROM contact WHERE contact_email = %s", 
            (contact_email,)
        )
        existing_contact = cursor.fetchone()

        if existing_contact:
            # Contact found, use existing ID
            contact_id_fk = existing_contact['contact_id']
        else:
            # B. Contact not found, insert new contact and retrieve its new ID
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
            # Fetch the ID generated by SERIAL
            contact_id_fk = cursor.fetchone()['contact_id']

        # --- MAIN RFQ INSERTION ---

        # 2. Prepare main data fields
        # Note: Psycopg2 handles Python's True/False correctly for PostgreSQL BOOLEAN.
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
            'contact_id_fk': contact_id_fk # The ID determined above
        }
        
        main_columns = ', '.join(main_fields.keys())
        main_placeholders = ', '.join(['%s'] * len(main_fields))
        main_values = list(main_fields.values())

        insert_main_sql = f"""
        INSERT INTO main ({main_columns})
        VALUES ({main_placeholders})
        """
        
        # 3. Execute main insertion
        cursor.execute(insert_main_sql, main_values)
        
        # 4. Commit the transaction if successful
        conn.commit()

        return jsonify({
            "status": "success", 
            "message": "RFQ data successfully stored.", 
            "rfq_id": rfq_id
        }), 200

    except ConnectionError as e:
        # Handled in get_db for connection issues
        return jsonify({"status": "error", "message": str(e)}), 500

    except OperationalError as e:
        if conn:
            conn.rollback()
            
        # Safely extract PostgreSQL error details
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
    
    # Get all query parameters
    rfq_id = request.args.get('rfq_id')
    customer_name = request.args.get('customer_name')
    product_line = request.args.get('product_line')
    
    # List to hold WHERE clause fragments and their corresponding values
    where_clauses = []
    query_params = []
    
    # --- 1. Dynamic WHERE Clause Construction ---
    
    if rfq_id:
        # Use exact match for RFQ ID
        where_clauses.append("m.rfq_id = %s")
        query_params.append(rfq_id)

    if customer_name:
        # Use case-insensitive partial match for Customer Name
        where_clauses.append("m.customer_name ILIKE %s")
        query_params.append(f"%{customer_name}%") # Add wildcards for LIKE/ILIKE

    if product_line:
        # Use exact match for Product Line
        where_clauses.append("m.product_line = %s")
        query_params.append(product_line)

    # Check if any filter was provided
    if not where_clauses:
        return jsonify({"status": "error", "message": "At least one search parameter (rfq_id, customer_name, or product_line) is required."}), 400

    # Combine clauses with 'AND'
    where_sql = " AND ".join(where_clauses)
    
    # --- 2. SQL Query Execution ---
    
    conn = None
    try:
        conn, cursor = get_db()
        
        # SQL to join main and contact tables and filter results
        # Alias 'm' for main and 'c' for contact
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

        # --- 3. Formatting Results (Optional, but recommended for clean API output) ---
        
        # Reformat the flat result rows into the original nested structure (main + nested contact)
        formatted_results = []
        for row in results:
            # Create a dictionary for the main RFQ data
            rfq_data = dict(row)
            
            # Extract contact fields and put them into a 'contact' dictionary
            contact = {
                'role': rfq_data.pop('contact_role', None),
                'email': rfq_data.pop('contact_email', None),
                'phone': rfq_data.pop('contact_phone', None)
            }
            # Remove the foreign key from the main object
            rfq_data.pop('contact_id_fk', None) 
            rfq_data.pop('contact_id', None) # Remove if joined table also returns its primary key
            
            # Add the nested contact object back
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
        # Log and rollback, though rollback isn't strictly necessary for a SELECT
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
        
        # --- NEW EXPLICIT SELECT QUERY ---
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
            # Filter by partial name match
            query += " WHERE product_name ILIKE %s"
            search_pattern = f"%{product_name}%"
            cursor.execute(query, (search_pattern,))
        else:
            # Get all products
            cursor.execute(query) 

        products_data = cursor.fetchall()
        
        # No need for base64 conversion here! 🎉

        # 2. Format and Send Response (200 OK)
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

# ----------------------------------------------------------------------

@app.route('/api/product-lines', methods=['GET'])
def retrieve_product_line_modified():
    """
    Retrieves product line data from the 'product_lines' table using a required ID.
    (Used to identify {Product_Line} automatically after product selection in Step 1)
    """
    # Changed parameter name to reflect the required foreign key
    product_line_id = request.args.get('productLineId') 

    # 1. Parameter Validation (Required: productLineId)
    if not product_line_id:
        return jsonify({
            "error": "Missing required query parameter: productLineId",
            "details": "The ID of the product line must be provided to retrieve its details."
        }), 400 

    conn = None
    try:
        conn, cursor = get_db()

        # 2. SQL Query: Exact match on the 'id' column
        query = """
            SELECT *
            FROM public.product_lines
            WHERE id = %s;
        """
        cursor.execute(query, (product_line_id,))

        product_line_data = cursor.fetchall()

        # 3. Format and Send Response (200 OK)
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
    This API does NOT require any parameters.
    """
    conn = None
    try:
        # Assurez-vous d'avoir une fonction get_db() qui retourne la connexion et le curseur
        # Make sure you have a working get_db() function returning connection and cursor
        conn, cursor = get_db()

        # 1. RequÃªte SQL pour obtenir uniquement les ID et les noms (ou texte)
        # SQL query to get only the ID and Name (or text)
        query = """
            SELECT id, name AS product_line_name, type_of_products AS description_snippet
            FROM public.product_lines
            ORDER BY id;
        """
        cursor.execute(query)

        product_lines_list = cursor.fetchall()

        # 2. Format et Envoi de la RÃ©ponse (200 OK)
        # Format and Send Response (200 OK)
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
    Useful when the client app only knows the product name and 
    needs to find the related product line info.
    """
    product_name = request.args.get('productName')

    # 1. Validate parameter
    if not product_name:
        return jsonify({
            "error": "Missing required query parameter: productName",
            "details": "Provide a valid product name to retrieve product-line details."
        }), 400

    conn = None
    cursor = None
    try:
        conn, cursor = get_db()

        # 2. Query to join products and product_lines tables
        # Using only columns that actually exist in both tables
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
    # When running locally, set the FLASK_APP environment variable.
    # On Azure, gunicorn will handle execution.
    app.run(debug=True)