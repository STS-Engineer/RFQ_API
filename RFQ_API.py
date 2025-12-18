import os
import time
import uuid
import datetime
import sys
import json
import re
import base64
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2 import OperationalError, errorcodes, extras
import requests
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, send_from_directory
from flask_mail import Mail, Message
from werkzeug.utils import secure_filename
from decimal import Decimal




# ------------------------ Environment Variables ------------------------
# Apollo API Key - hardcoded for simplicity
ENV_API_KEY = "WsqN_6KDO9kkRBt1ZvkDDw"
ALLOW_HEADER_OVERRIDE = os.environ.get("ALLOW_HEADER_OVERRIDE", "true").lower() in ("1", "true", "yes")

# --- 1. CONFIGURATION ---
# PostgreSQL configuration
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "Costing_DB",
    "user": "administrationSTS",
    "password": "St$@0987"
}
RFQ_DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}
CLIENT_DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "Client_DB", # Client/Organization data (Groupe, Unit, Person)
    "user": "administrationSTS",
    "password": "St$@0987"
}


app = Flask(__name__)

# URL Configuration for Validation Workflow
BASE_URL = "https://rfq-api.azurewebsites.net" 
RFQ_SUBMISSION_API_URL = "https://rfq-api.azurewebsites.net/api/rfq/submit"

# Flask-Mail Configuration for Outlook SMTP (UNAUTHENTICATED RELAY)
app.config['MAIL_SERVER'] = 'avocarbon-com.mail.protection.outlook.com'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False 
app.config['MAIL_DEFAULT_SENDER'] = 'administration.STS@avocarbon.com'

mail = Mail(app)

# Monday.com Configuration
MONDAY_API_URL = "https://api.monday.com/v2" 
MONDAY_API_TOKEN ="eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjU3MTg5ODk4NSwiYWFpIjoxMSwidWlkIjo3NjIxOTg5NSwiaWFkIjoiMjAyNS0xMC0wOVQwNzo1NToyMi4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDUyNTc0NywicmduIjoidXNlMSJ9.olEVa7_wuCFJaFuYU1Qp3A8JEuyq9vQihAdA2WVL6yA" 

# ------------------------ File Upload Configuration ------------------------
# Define the upload folder, MUST be relative to the application's root or an absolute path
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'rfq_files')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'jpg', 'jpeg', 'png', 'zip', 'xls', 'xlsx'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024 # 16 Megabytes

app.add_url_rule('/rfq_files/<path:filename>', 
                 endpoint='uploaded_file', 
                 view_func=lambda filename: send_from_directory(app.config['UPLOAD_FOLDER'], filename))


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ------------------------ Validation Functions ------------------------
# ------------------------ Validation Functions ------------------------
def validate_search_request(data: dict) -> tuple:
    """Validates search people request data. Returns (is_valid, error_message, validated_data)"""
    if not data:
        return False, "No data provided", None
    
    if "q_organization_name" not in data:
        return False, "q_organization_name is required", None
    
    if not isinstance(data["q_organization_name"], list):
        return False, "q_organization_name must be a list", None
    
    if not data["q_organization_name"]:
        return False, "q_organization_name cannot be empty", None
    
    # Set defaults
    validated = {
        "q_organization_name": data["q_organization_name"],
        "person_titles": data.get("person_titles"),
        "person_seniorities": data.get("person_seniorities"),
        "organization_num_employees_ranges": data.get("organization_num_employees_ranges"),
        "q_organization_domains": data.get("q_organization_domains"),
        "page": int(data.get("page", 1)),
        "per_page": int(data.get("per_page", 25)),
        "delay_between_requests": float(data.get("delay_between_requests", 1.0))
    }
    
    # Validate ranges
    if validated["page"] < 1 or validated["page"] > 500:
        return False, "page must be between 1 and 500", None
    
    if validated["per_page"] < 1 or validated["per_page"] > 100:
        return False, "per_page must be between 1 and 100", None
    
    if validated["delay_between_requests"] < 0:
        return False, "delay_between_requests must be >= 0", None
    
    return True, None, validated

def validate_enrich_request(data: dict) -> tuple:
    """Validates enrich person request. Returns (is_valid, error_message, validated_data)"""
    if not data:
        return False, "No data provided", None
    
    validated = {
        "first_name": data.get("first_name"),
        "last_name": data.get("last_name"),
        "name": data.get("name"),
        "organization_name": data.get("organization_name"),
        "domain": data.get("domain"),
        "email": data.get("email"),
        "id": data.get("id"),
        "linkedin_url": data.get("linkedin_url"),
        "reveal_personal_emails": bool(data.get("reveal_personal_emails", False)),
        "reveal_phone_number": bool(data.get("reveal_phone_number", False)),
        "webhook_url": data.get("webhook_url")
    }
    
    return True, None, validated
# Add these filter functions after your validation functions and before the Apollo routes

def filter_search_contact(contact: dict) -> dict:
    """
    Filters a contact from search results to return only essential fields.
    Returns: name, title, email, linkedin_url, organization_name
    """
    organization = contact.get('organization', {}) or {}
    
    return {
        "name": contact.get('name'),
        "title": contact.get('title'),
        "email": contact.get('email'),
        "linkedin_url": contact.get('linkedin_url'),
        "organization_name": organization.get('name') if isinstance(organization, dict) else None
    }


def filter_enrich_contact(response_data: dict) -> dict:
    """
    Filters enriched person data to return only essential fields.
    Returns: first_name, last_name, name, title, email, linkedin_url, phone_numbers, organization.name
    """
    person = response_data.get('person', {})
    
    if not person:
        return response_data  # Return as-is if no person data
    
    # Filter phone numbers
    phone_numbers = person.get('phone_numbers', [])
    filtered_phones = []
    if isinstance(phone_numbers, list):
        for phone in phone_numbers:
            if isinstance(phone, dict):
                filtered_phones.append({
                    "raw_number": phone.get('raw_number'),
                    "sanitized_number": phone.get('sanitized_number'),
                    "type": phone.get('type')
                })
    
    # Filter organization
    organization = person.get('organization', {})
    filtered_org = None
    if isinstance(organization, dict):
        filtered_org = {
            "name": organization.get('name')
        }
    
    return {
        "person": {
            "first_name": person.get('first_name'),
            "last_name": person.get('last_name'),
            "name": person.get('name'),
            "title": person.get('title'),
            "email": person.get('email'),
            "linkedin_url": person.get('linkedin_url'),
            "phone_numbers": filtered_phones,
            "organization": filtered_org
        }
    }
def filter_enrich_contact_bulk(entry: dict) -> dict:
    """
    Supporte le format 'bulk_match' (objet plat), et renvoie une structure uniforme.
    Tolère les éléments None dans matches[].
    """
    if not isinstance(entry, dict):
        return {"person": None, "status": "no_match"}

    org = entry.get("organization") or {}
    return {
        "person": {
            "first_name": entry.get("first_name"),
            "last_name": entry.get("last_name"),
            "name": entry.get("name"),
            "title": entry.get("title"),
            "email": entry.get("email"),
            "linkedin_url": entry.get("linkedin_url"),
            "phone_numbers": [],  # (non présent dans ton exemple bulk)
            "organization": {"name": org.get("name")} if isinstance(org, dict) else None,
        }
    }
def validate_bulk_enrich_request(data: dict) -> tuple:
    """Validates bulk enrich request. Returns (is_valid, error_message, validated_data)"""
    if not data:
        return False, "No data provided", None
    
    if "details" not in data:
        return False, "details field is required", None
    
    if not isinstance(data["details"], list):
        return False, "details must be a list", None
    
    if len(data["details"]) > 10:
        return False, "details can contain at most 10 items", None
    
    validated = {
        "details": data["details"],
        "reveal_personal_emails": bool(data.get("reveal_personal_emails", False)),
        "reveal_phone_number": bool(data.get("reveal_phone_number", False)),
        "webhook_url": data.get("webhook_url")
    }
    
    return True, None, validated
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
    Use provided key if ALLOW_HEADER_OVERRIDE=true, otherwise use environment key.
    """
    if ALLOW_HEADER_OVERRIDE and api_key:
        return ApolloClient(api_key)
    if not ENV_API_KEY:
        raise RuntimeError("APOLLO_API_KEY is not set. Add it to your environment variables.")
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

# --- VALIDATION WORKFLOW: PERSISTENT STATE MANAGEMENT ---
def get_request_data(request_id):
    """Retrieves request data regardless of status."""
    conn = None
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT data FROM pending_validations WHERE request_id = %s;", (request_id,))
        result = cursor.fetchone()

        if result and 'data' in result:
            return result['data'] 
        return None
    except ConnectionError:
        return None 
    except Exception as e:
        sys.stderr.write(f"DB GET FAILED: {e}\n")
        return None
    finally:
        if conn:
            conn.close()

def set_request_data(request_id, data):
    """Stores or updates data into the pending_validations table."""
    conn = None
    try:
        conn, cursor = get_db()

        insert_sql = """
            INSERT INTO pending_validations (request_id, data, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id) DO UPDATE
            SET data = EXCLUDED.data, status = EXCLUDED.status, created_at = NOW();
        """
        cursor.execute(insert_sql, (request_id, json.dumps(data), data['status'])) 
        conn.commit()
    except Exception as e:
        sys.stderr.write(f"DB SET FAILED: {e}\n")
        if conn: conn.rollback()
    finally:
        if conn:
            conn.close()

def delete_request_data(request_id):
    """Deletes request data (optional, but good for cleanup once validated)."""
    conn = None
    try:
        conn, cursor = get_db()
        cursor.execute("DELETE FROM pending_validations WHERE request_id = %s;", (request_id,))
        conn.commit()
    except Exception as e:
        sys.stderr.write(f"DB DELETE FAILED: {e}\n")
    finally:
        if conn:
            conn.close()

# --- VALIDATION WORKFLOW: MAIL SENDING ---
def safe_send_mail(msg):
    """Synchronously sends email using Flask-Mail context."""
    try:
        print(f"DEBUG: Attempting SMTP connection to {app.config['MAIL_SERVER']}:{app.config['MAIL_PORT']} (TLS={app.config['MAIL_USE_TLS']}) - NO AUTHENTICATION")
        with app.app_context():
            mail.send(msg)
        print(f"DEBUG: Mail successfully sent to {msg.recipients}")
        return True, None
    except Exception as e:
        sys.stderr.write(f"FATAL MAIL SEND ERROR: {e}\n")
        return False, str(e)

# --- VALIDATION WORKFLOW: MONDAY.COM INTEGRATION ---
def _normalize_str(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def map_customer_area(input_value: str, country: Optional[str] = None) -> Optional[str]:
    """
    Maps user inputs to Monday Status labels for 'Customer Area'.
    Allowed labels: 'Asia', 'Korea/Japan', 'India', 'North America', 'Europe', 'South America'
    """
    v = _normalize_str(input_value)
    c = _normalize_str(country) if country else None

    # Exact board labels
    exact = {
        "asia": "Asia",
        "korea/japan": "Korea/Japan",
        "india": "India",
        "north america": "North America",
        "europe": "Europe",
        "south america": "South America",
    }
    if v in exact:
        return exact[v]

    # Canonical inputs you said you'll receive + common aliases
    alias = {
        "nafta": "North America",
        "north-america": "North America",
        "n. america": "North America",
        "amer": "North America",
        "na": "North America",

        "europe": "Europe",
        "eu": "Europe",

        "south america": "South America",
        "south-america": "South America",
        "s. america": "South America",
        "latam": "South America",

        "apac": "Asia",
        "east asia": "Asia",
        "south asia": "Asia",
        "se asia": "Asia",
        "sea": "Asia",
    }

    if v in alias:
        candidate = alias[v]
        # Refinements by country for East/South Asia
        if v in {"apac", "east asia"} and c:
            if c in {"jp", "japan", "kr", "korea", "south korea"}:
                return "Korea/Japan"
            return "Asia"
        if v == "south asia" and c:
            if c in {"in", "india"}:
                return "India"
            return "Asia"
        return candidate

    # Country-based fallbacks
    if v in {"jp", "japan", "kr", "korea", "south korea"}:
        return "Korea/Japan"
    if v in {"in", "india"}:
        return "India"

    # Keyword fallbacks
    if "east asia" in v:
        return "Korea/Japan" if c in {"jp","japan","kr","korea","south korea"} else "Asia"
    if "south asia" in v:
        return "India" if c in {"in","india"} else "Asia"
    if "europe" in v:
        return "Europe"
    if "nafta" in v or "north america" in v or "usa" in v or "canada" in v or "mexico" in v:
        return "North America"
    if "south america" in v or "latam" in v:
        return "South America"

    return None


# Customer Area label ID mapping (from Monday board)

# --- ADD THIS FUNCTION ABOVE create_monday_rfq_item ---
def map_zone_responsible_email(zone_name: str) -> Optional[str]:
    """Maps a normalized zone name to the corresponding Zone Responsible email."""
    v = (zone_name or "").strip().lower()
    
    # Use the normalized names derived from map_customer_area
    if "korea/japan" in v or "east asia" in v or "asia" in v:
        return "tao.ren@avocarbon.com"
    elif "india" in v or "south asia" in v:
        return "eipe.thomas@avocarbon.com"
    elif "europe" in v or "eu" in v:
        return "franck.lagadec@avocarbon.com"
    elif "south america" in v or "north america" in v or "nafta" in v:
        return "dean.hayward@avocarbon.com"
    # Note: If 'Asia' is the result of normalization but isn't explicitly 'East' or 'South', 
    # it currently falls back to Tao Ren, which aligns with the first mapping (East Asia/Korea/Japan) if only 'Asia' is provided.
    
    return None

def _monday_query(query: str, variables: dict | None = None) -> dict:
    headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    r = requests.post(MONDAY_API_URL, json=payload, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"Monday GraphQL error: {data['errors']}")
    return data["data"]

def get_user_id_by_email(email: str) -> int | None:
    """
    Resolve a monday user ID from an email.
    We fetch pages of users and match email client-side to avoid schema changes.
    """
    page = 1
    while True:
        q = """
        query ($limit:Int!, $page:Int!){
          users (limit:$limit, page:$page) {
            id
            name
            email
            is_guest
          }
        }"""
        data = _monday_query(q, {"limit": 200, "page": page})
        users = data.get("users") or []
        if not users:
            return None
        for u in users:
            if (u.get("email") or "").strip().lower() == (email or "").strip().lower():
                return int(u["id"])
        page += 1

CUSTOMER_AREA_LABEL_IDS = {
    "asia": 0,
    "korea/japan": 1,
    "india": 2,
    "north america": 3,
    "east asia": 4,
    "europe" : 105,
    "south america" : 156
}


def map_customer_area_to_id(value: str) -> int | None:
    """
    Maps input strings like 'Asia', 'AMER', 'APAC', etc. to their label IDs.
    Returns None if no valid mapping is found.
    """
    v = (value or "").strip().lower()

    # Direct match if using existing labels
    if v in CUSTOMER_AREA_LABEL_IDS:
        return CUSTOMER_AREA_LABEL_IDS[v]

    # Common synonyms
    alias = {
        "amer": "north america",
        "nafta": "north america",
        "na": "north america",
        "north-america": "north america",
        "apac": "asia",
        "east asia": "east asia",
        "south asia": "india",  # depending on your board config
        "emea": "asia",         # fallback if not separately labeled
    }

    if v in alias and alias[v] in CUSTOMER_AREA_LABEL_IDS:
        return CUSTOMER_AREA_LABEL_IDS[alias[v]]

    return None



def create_monday_rfq_item(rfq_data: Dict[str, Any], report_content: str, request_data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    NEW_COLUMN_IDS = {
        "customer_name_col": "text_mkwr9zq7",
        "application": "text_mkwx7nrf",        # Dropdown
        "product_line": "color_mkspxcz1",          # Status/Color
        "sop_year": "numeric_mksp7jqy",
        "target_price_eur": "numeric_mkszczs",
        "delivery_zone": "color_mksydtj9",         # Status/Color
        "overall_feasibility": "long_text_mkws5y8x",
        "kam_col": "multiple_person_mkszcpvx",       # KAM - Mapped to request_data['user_email']
        "zone_responsible_col": "multiple_person_mkszd4qb", # Zone Manager/Responsible - Mapped via logic
        "validator_col": "multiple_person_mkt1vhsa",# Validator - Mapped to request_data['validator_email']
        "qty_per_year_kp": "numeric_mkszvbyb",
        "rfq_stage": "color_mksysrr6"
    }
    AI_REPORT_COLUMN_ID = "long_text_mkwh4mee"

    MONDAY_API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjU3MTg5ODk4NSwiYWFpIjoxMSwidWlkIjo3NjIxOTg5NSwiaWFkIjoiMjAyNS0xMC0wOVQwNzo1NToyMi4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6NDUyNTc0NywicmduIjoidXNlMSJ9.olEVa7_wuCFJaFuYU1Qp3A8JEuyq9vQihAdA2WVL6yA"  # 🔒 never hardcode secrets
    MONDAY_API_URL = "https://api.monday.com/v2"
    BOARD_ID = "9550168457"

    if not MONDAY_API_TOKEN:
        return False, "Monday API Token not configured."

    # ✅ Allowed labels (exactly as configured on the board)
    allowed_product_line_labels = {"Assembly","Friction","Injection","Chokes","Brushes","Seals"}
    
    customer_name = rfq_data.get('customer_name', 'Unnamed RFQ')
    safe_item_name = json.dumps(f"{customer_name} RFQ")  # ensures proper escaping in GraphQL
    raw_area = rfq_data.get("delivery_zone") or rfq_data.get("customer_area") or ""
    
    
    country_hint = rfq_data.get("country")  # optional, if you have it
    mapped_area = map_customer_area(raw_area, country_hint)
    if not mapped_area:
        return False, ("Invalid Customer Area "
                    f"'{raw_area}'. Allowed: Asia, Korea/Japan, India, North America, Europe, South America")

    # Determine Zone Responsible Email based on MAPPED Area Label
    zone_responsible_email = map_zone_responsible_email(mapped_area)
    if not zone_responsible_email:
        return False, f"Could not map Zone Responsible email for area: {mapped_area}"
        
    kam_email = request_data.get('user_email')
    validator_email_to_set = request_data.get('validator_email')
    
    if not kam_email:
        return False, "Missing user_email (KAM) in request_data."
    if not validator_email_to_set:
        return False, "Missing validator_email in request_data."



    # --- Resolve People → IDs ---
    kam_id = get_user_id_by_email(kam_email)
    validator_id = get_user_id_by_email(validator_email_to_set)
    zone_responsible_id = get_user_id_by_email(zone_responsible_email)

    missing = []
    if kam_id is None: missing.append(f"KAM '{kam_email}'")
    if validator_id is None: missing.append(f"Validator '{validator_email_to_set}'")
    if zone_responsible_id is None: missing.append(f"Zone Responsible '{zone_responsible_email}'")
    if missing:
        return False, "Unknown monday user(s): " + ", ".join(missing)


    # --- CALCULATE QTY PER YEAR (kP) ---
    raw_annual_volume = rfq_data.get('annual_volume')
    
    qty_kp = None
    if raw_annual_volume is not None:
        try:
            # 1. Convert to float/int
            volume_numeric = float(raw_annual_volume)
            # 2. Divide by 1000 to get thousands of pieces (kP)
            qty_kp = volume_numeric / 1000
            # 3. Convert back to string for Monday payload
            qty_kp_str = str(qty_kp)
        except ValueError:
            # Handle case where annual_volume is not a valid number
            print(f"WARNING: annual_volume '{raw_annual_volume}' is not numeric. Sending 0 or blank.")
            qty_kp_str = "0"
    else:
        qty_kp_str = ""
    # -----------------------------------


    # --- Validate / normalize inputs ---
    raw_pl = (rfq_data.get('product_line') or "").strip()
    if raw_pl not in allowed_product_line_labels:
        return False, (f"Invalid product_line '{raw_pl}'. Allowed: {sorted(allowed_product_line_labels)}")

    raw_area = (rfq_data.get("delivery_zone") or rfq_data.get("customer_area") or "").strip()
    area_label_id = map_customer_area_to_id(raw_area)

    if area_label_id is None:
        return False, (f"Invalid Customer Area '{raw_area}'. "
                    f"Allowed keys: {list(CUSTOMER_AREA_LABEL_IDS.keys())}")

    application_value = (rfq_data.get('application') or "").strip()
    if not application_value:
        return False, "Missing application"

    # --- BUILD COLUMN VALUES ---
    column_values = {
        NEW_COLUMN_IDS["customer_name_col"]: customer_name,
        NEW_COLUMN_IDS["sop_year"]: str(rfq_data.get('sop_year') or ""),
        NEW_COLUMN_IDS["target_price_eur"]: str(rfq_data.get('target_price_eur') or ""),

        # Dropdown expects {"labels": ["..."]} and label must exist on the column
        NEW_COLUMN_IDS["application"]: application_value,
        NEW_COLUMN_IDS["qty_per_year_kp"]: qty_kp_str,
        # Status/Color expects an existing label
        NEW_COLUMN_IDS["product_line"]: {"label": raw_pl},
        NEW_COLUMN_IDS["delivery_zone"]: {"index": area_label_id},

        NEW_COLUMN_IDS["overall_feasibility"]: {"text": rfq_data.get('overall_feasibility') or ""},
        AI_REPORT_COLUMN_ID: {"text": report_content or ""},
        NEW_COLUMN_IDS["rfq_stage"]: {"label": "In Costing"},
        # --- PEOPLE COLUMNS (Mandatory structure: {"personsAndTeams": [{"id": email_string, "kind": "email"}]}) ---
        # 1. KAM (User Email)
        NEW_COLUMN_IDS["kam_col"]: {
    "personsAndTeams": [{"id": kam_id, "kind": "person"}]
        },
        NEW_COLUMN_IDS["zone_responsible_col"]: {
            "personsAndTeams": [{"id": zone_responsible_id, "kind": "person"}]
        },
        NEW_COLUMN_IDS["validator_col"]: {
            "personsAndTeams": [{"id": validator_id, "kind": "person"}]
        }
    }

    column_values_json = json.dumps(column_values)

    mutation = f"""
        mutation {{
            create_item(
                board_id: {BOARD_ID},
                item_name: {safe_item_name},
                column_values: {json.dumps(column_values_json)}
            ) {{
                id
            }}
        }}
    """

    headers = {
        "Authorization": MONDAY_API_TOKEN,  # Monday expects the raw token (no 'Bearer')
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(MONDAY_API_URL, json={"query": mutation}, headers=headers, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
        if "errors" in payload:
            return False, f"GraphQL Error: {payload['errors']}"
        return True, payload["data"]["create_item"]["id"]
    except requests.exceptions.RequestException as e:
        return False, f"Monday.com API Request Failed: {e}"
    except Exception as e:
        return False, f"Unexpected Error during Monday item creation: {e}"




BOARD_ID = "9550168457" 


COLUMN_IDS_RETRIEVAL = {
    "kam": "multiple_person_mkszcpvx",         # Key Account Manager (Likely a People Column)
    "zone_manager": "multiple_person_mkszd4qb",   # Zone Manager (Likely a People Column)
    "vp_sales": "multiple_person_mkt0hwt1",       # VP Sales (Likely a People Column)
    "ceo": "multiple_person_mkt1jh5b"            # CEO (Likely a People Column)
}





def _monday_post(query: str, variables: dict):
    resp = requests.post(
        MONDAY_API_URL,
        json={"query": query, "variables": variables},
        headers={
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
            "API-Version": "2024-04",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        # surface monday GraphQL errors so your route can report 502 instead of 404
        raise RuntimeError(data["errors"])
    return data["data"]

def _get_user_map(user_ids: List[int]) -> Dict[int, Dict[str, str]]:
    """Return {user_id: {name, email}} for monday user IDs."""
    if not user_ids:
        return {}
    q = """
    query($ids:[ID!]) {
      users(ids:$ids) { id name email }
    }
    """
    d = _monday_post(q, {"ids": user_ids})
    return {int(u["id"]): {"name": u.get("name"), "email": u.get("email")} for u in d.get("users", [])}

def get_monday_data_by_project_id(item_id: int) -> Optional[Dict[str, str]]:
    """
    Retrieves KAM, Zone Manager, VP Sales, CEO names (from column text) and emails (via users()).
    """
    try:
        if not MONDAY_API_TOKEN or not MONDAY_API_URL:
            print("FATAL: Monday API Token or URL is not configured.")
            return None
    except NameError:
        print("FATAL: MONDAY_API_TOKEN or MONDAY_API_URL is not defined.")
        return None

    col_id_map = {
        "kam": COLUMN_IDS_RETRIEVAL["kam"],
        "zone_manager": COLUMN_IDS_RETRIEVAL["zone_manager"],
        "vp_sales": COLUMN_IDS_RETRIEVAL["vp_sales"],
        "ceo": COLUMN_IDS_RETRIEVAL["ceo"],
    }
    col_ids = list(col_id_map.values())

    # ✅ Corrected GraphQL: no person{email}; only persons_and_teams { id, kind }
    query_item = """
    query($boardId: ID!, $itemId: [ID!], $colIds: [String!]) {
      boards(ids: [$boardId]) {
        items_page(query_params: { ids: $itemId }) {
          items {
            id
            name
            column_values(ids: $colIds) {
              id
              text
              value
              ... on PeopleValue {
                persons_and_teams {
                  id
                  kind   # "person" or "team"
                }
              }
            }
          }
        }
      }
    }
    """
    variables = {"boardId": BOARD_ID, "itemId": [str(item_id)], "colIds": col_ids}

    try:
        d = _monday_post(query_item, variables)
    except Exception as e:
        print("ERROR: monday.com API returned errors:")
        print(e)
        return None

    items = d.get("boards", [{}])[0].get("items_page", {}).get("items", [])
    if not items:
        print(f"No item found with ID {item_id}.")
        return None

    item = items[0]
    extracted: Dict[str, str] = {"project_id": item["id"], "project_name": item["name"]}

    # collect user IDs to fetch emails in one go
    need_user_ids: Dict[str, int] = {}
    all_ids: List[int] = []

    for col in item.get("column_values", []):
        role = next((k for k, v in col_id_map.items() if v == col["id"]), None)
        if not role:
            continue

        # board cell display text
        extracted[f"{role}_name"] = col.get("text") or "N/A"
        extracted[f"{role}_email"] = "N/A"  # default

        pt = col.get("persons_and_teams")
        # fallback: parse JSON in .value if SDK didn’t hydrate persons_and_teams
        if not pt and col.get("value"):
            try:
                val = json.loads(col["value"])
                pt = val.get("personsAndTeams")
            except Exception:
                pt = None

        if isinstance(pt, list):
            # choose first person (ignore teams; they don’t have emails)
            for ent in pt:
                if ent.get("kind") == "person" and ent.get("id") is not None:
                    uid = int(ent["id"])
                    need_user_ids[role] = uid
                    all_ids.append(uid)
                    break

    # fetch emails once and map back
    user_map = _get_user_map(sorted(set(all_ids)))
    for role, uid in need_user_ids.items():
        info = user_map.get(uid)
        if info:
            extracted[f"{role}_email"] = info.get("email") or "N/A"
            # If you prefer directory name over board text, uncomment:
            # extracted[f"{role}_name"] = info.get("name") or extracted[f"{role}_name"]

    return extracted






@app.route('/api/project/retrieve/<int:project_id>', methods=['GET'])
def retrieve_project_data(project_id):
    """
    API route to retrieve KAM, ZM, VP Sales, and CEO names for a project ID.
    Example: GET /api/project/retrieve/1234567890
    """
    data = get_monday_data_by_project_id(project_id)
    
    if data:
        return jsonify({
            "status": "success",
            "data": data
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": f"Project ID {project_id} not found or API error occurred."
        }), 404








@app.route('/health')
def health():
    return {"status": "healthy"}

# ======================== VALIDATION WORKFLOW ENDPOINTS ========================




@app.route('/api/send-report', methods=['POST', 'PUT'])
def send_report_for_validation():
    """Sends AI report for validation via email."""
    print(f"DEBUG: Endpoint /api/send-report hit with method: {request.method}")
    data = request.json

    # 1. Get Data from Assistant
    report_content = data.get('report')
    user_email = data.get('user_email')
    validator_email = data.get('validator_email')
    rfq_payload = data.get('rfq_payload')
    #project_id = data.get('project_id')
    rfq_file_path = data.get('rfq_file_path')
    if not all([report_content, user_email, validator_email, rfq_payload]):
        print("DEBUG: Missing required data.")
        return jsonify({"message": "Missing required data (report, user_email, validator_email, rfq_payload)"}), 400

    # 2. Store State and Generate Token
    request_id = str(uuid.uuid4())
    request_data = {
        'report_content': report_content,
        'user_email': user_email,
        'validator_email': validator_email,
        'rfq_payload': rfq_payload,
        'rfq_file_path': rfq_file_path,
        #'project_id': project_id, 
        'status': 'PENDING',
        'validator_comments': None,
        'created_at': datetime.datetime.now().isoformat()
    }
    set_request_data(request_id, request_data)

    # 3. Create Interactive Links
    confirm_link = f"{BASE_URL}/validate-page?id={request_id}&action=confirm"
    decline_link = f"{BASE_URL}/validate-page?id={request_id}&action=decline"
    update_link = f"{BASE_URL}/validate-page?id={request_id}&action=update"  # <--- NEW LINK
    # 3. Create Interactive Links
    # --- NEW FILE HANDLING LOGIC ---
    file_display = ""
    
    # Check if rfq_file_path is a list (new format) or string (old format)
    paths_to_process = []
    if isinstance(rfq_file_path, list):
        paths_to_process = rfq_file_path
    elif isinstance(rfq_file_path, str) and rfq_file_path:
        paths_to_process = [rfq_file_path]

    if paths_to_process:
        file_links = []
        for idx, path in enumerate(paths_to_process):
            # Clean path
            path_in_repo = path.lstrip('/')
            file_url = f"https://raw.githubusercontent.com/STS-Engineer/RFQ-back/main/{path_in_repo}"
            
            # Create a nice link, e.g., "Download File 1"
            file_links.append(f'<a href="{file_url}" target="_blank">Download File {idx + 1}</a>')
        
        # Join links with a separator
        links_html = " | ".join(file_links)
        file_display = f'<p style="margin-top: 15px;"><strong>Attached Documents:</strong> {links_html}</p>'
    # -------------------------------
    # 4. Construct Email
    email_body_html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height: 1.6;">
        <h2>Action Required: AI RFQ Validation</h2>
        <p>A report generated by the AI assistant requires your review. Please confirm, request updates, or decline.</p>
        <hr>
        <div style="padding: 15px; border: 1px solid #ccc; background-color: #f9f9f9; border-radius: 4px;">
            <strong>Report Content:</strong>
            <pre style="white-space: pre-wrap; font-family: monospace; font-size: 14px; margin: 10px 0;">{report_content}</pre>
        </div>
        {file_display}
        <hr>
        <table width="100%" border="0" cellspacing="0" cellpadding="0">
            <tr>
                <td align="center">
                    <table border="0" cellspacing="0" cellpadding="0" style="margin: 20px 0;">
                        <tr>
                            <td align="center" style="border-radius: 4px;" bgcolor="#4CAF50">
                                <a href="{confirm_link}" target="_blank" style="font-size: 14px; font-weight: bold; font-family: Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; padding: 12px 18px; border-radius: 4px; border: 1px solid #4CAF50; display: inline-block;">CONFIRM</a>
                            </td>
                            <td width="15"></td>
                            <td align="center" style="border-radius: 4px;" bgcolor="#2196F3">
                                <a href="{update_link}" target="_blank" style="font-size: 14px; font-weight: bold; font-family: Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; padding: 12px 18px; border-radius: 4px; border: 1px solid #2196F3; display: inline-block;">REQUEST UPDATE</a>
                            </td>
                            <td width="15"></td>
                            <td align="center" style="border-radius: 4px;" bgcolor="#F44336">
                                <a href="{decline_link}" target="_blank" style="font-size: 14px; font-weight: bold; font-family: Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; padding: 12px 18px; border-radius: 4px; border: 1px solid #F44336; display: inline-block;">DECLINE</a>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
        <p><small>Initiated by: {user_email}</small></p>
    </body></html>"""
    # 5. Send Email
    msg = Message("AI RFQ Validation Required", recipients=[validator_email])
    msg.html = email_body_html
    ok, err = safe_send_mail(msg)

    if not ok:
        return jsonify({"message": "Failed to send email via SMTP.", "error": err}), 500

    print(f"DEBUG: Validation request created with ID: {request_id}")
    return jsonify({"message": "Validation email sent successfully.", "request_id": request_id, "rfq_file_path": rfq_file_path}), 200












@app.route('/validate-page')
def validate_page():
    """Landing page for validator to view report and enter comments."""
    print(f"DEBUG: Endpoint /validate-page hit. Args: {request.args}")
    request_id = request.args.get('id')
    action = request.args.get('action') 

    # Retrieval from PostgreSQL
    request_data = get_request_data(request_id)

    if request_data is None:
        return "Error: Invalid or expired validation request.", 404

    report_content = request_data['report_content']
    status = request_data['status']
    validated_at = request_data.get('validated_at', 'N/A')
    comments = request_data.get('validator_comments', 'No comments provided.')

    if status != 'PENDING':
        # Already Processed
        final_action = status
        color = '#4CAF50' if final_action == 'CONFIRMED' else '#F44336'

        return f"""
        <!DOCTYPE html><html lang="en"><head><title>Processed</title>
        <style>
            body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f4; }} 
            .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15); border-left: 6px solid {color}; }} 
            h1 {{ color: {color}; margin-bottom: 20px; }} 
            h3 {{ margin-top: 25px; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
            .comment-box {{ background-color: #f0f8ff; border: 1px solid #cceeff; padding: 15px; border-radius: 4px; text-align: left; }}
        </style>
        </head>
        <body>
            <div class="card">
                <h1>Report Already {final_action}!</h1>
                <p style="font-size: 1.1em;">This report was previously finalized by a validator.</p>
                
                <h3>Decision Details</h3>
                <p><strong>Action Taken:</strong> <span style="color: {color}; font-weight: bold;">{final_action}</span></p>
                <p><strong>Processed On:</strong> {validated_at}</p>
                
                <h3>Validator Comments</h3>
                <div class="comment-box">
                    {comments}
                </div>
                <p style="margin-top: 20px;"><a href="/">Return to start</a></p>
            </div>
        </body>
        </html>
        """, 200

    # Status is PENDING - Show form
    if action == 'confirm':
        title, action_text, color, comments_label = "Confirm AI Report", "Confirm", "#4CAF50", "Validator Comments (Optional):"
    elif action == 'decline':
        title, action_text, color, comments_label = "Decline AI Report", "Decline", "#F44336", "Reason for Decline (Required):"
    elif action == 'update':
        title, action_text, color, comments_label = "Request Update", "Submit Update Request", "#2196F3", "Describe the required changes (Required):"
    else:
        return "Error: Invalid action specified.", 400

    

    html_content = f"""
    <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{title}</title>
        <style>
            body {{ font-family: 'Inter', sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }} 
            .container {{ max-width: 800px; margin: 0 auto; background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15); }} 
            h1 {{ color: {color}; text-align: center; border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 25px; }} 
            .report-box {{ background-color: #f0f8ff; border: 1px solid #cceeff; padding: 15px; border-radius: 8px; margin-bottom: 20px; white-space: pre-wrap; font-family: monospace; font-size: 0.95em; }} 
            label {{ display: block; margin-bottom: 10px; font-weight: bold; color: #333; }} 
            textarea {{ width: 100%; padding: 12px; box-sizing: border-box; border: 2px solid #ccc; border-radius: 6px; resize: vertical; min-height: 150px; transition: border-color 0.3s; }} 
            textarea:focus {{ border-color: {color}; outline: none; }} 
            .submit-btn {{ background-color: {color}; color: white; padding: 14px 25px; border: none; border-radius: 6px; cursor: pointer; font-size: 17px; font-weight: bold; width: 100%; margin-top: 20px; transition: background-color 0.3s, transform 0.1s; }} 
            .submit-btn:hover {{ opacity: 0.9; transform: translateY(-1px); }} 
        </style>
    </head>
    <body><div class="container">
        <h1>{title}</h1>
        
        <h2>AI Report Content</h2>
        <div class="report-box">{report_content}</div>
        
        <form action="/api/handle-validation" method="post">
            <input type="hidden" name="request_id" value="{request_id}">
            <input type="hidden" name="action" value="{action}">
            
            <label for="comments">{comments_label}</label>
            <textarea id="comments" name="comments" placeholder="Type here..." required></textarea>
            
            <button type="submit" class="submit-btn">{action_text}</button>
        </form>
        <p style="text-align: center; margin-top: 30px;"><small>Report ID: {request_id}</small></p>
    </div></body></html>"""
    
    return html_content


@app.route('/api/handle-validation', methods=['POST'])
def handle_validation():
    """Processes validator's decision and submits to database."""
    print(f"DEBUG: Endpoint /api/handle-validation hit with method: {request.method}")

    request_id = request.form.get('request_id')
    action = request.form.get('action')
    comments = request.form.get('comments')
    # Retrieval from PostgreSQL
    request_data = get_request_data(request_id)

    if request_data is None:
        return "Error: Invalid or expired validation request.", 404

    if request_data['status'] != 'PENDING':
        return "Error: This report has already been processed.", 400

    # 1. Update Persistent State
    request_data['status'] = action.upper()
    request_data['validator_comments'] = comments
    request_data['validated_at'] = datetime.datetime.now().isoformat()
    set_request_data(request_id, request_data)

    # Get the emails from the stored request data
    created_by_email = request_data.get('user_email')
    validated_by_email = request_data.get('validator_email')

    # 2. Prepare Final RFQ Payload
    original_rfq_payload = request_data.get('rfq_payload')
    rfq_file_path = request_data.get('rfq_file_path')

    if action == 'update':
        conn = None
        try:
            conn, cursor = get_db()
            
            # Use the existing contact ID if present, otherwise handle contact logic
            contact_data = original_rfq_payload.get('contact', {})
            contact_id_fk = None
            
            # Try to resolve Contact ID
            if contact_data.get('email'):
                cursor.execute("SELECT contact_id FROM contact WHERE contact_email = %s", (contact_data['email'],))
                res = cursor.fetchone()
                if res:
                    contact_id_fk = res['contact_id']
                else:
                    # Create contact on the fly if needed for the update request
                     cursor.execute(
                        "INSERT INTO contact (contact_role, contact_email, contact_phone) VALUES (%s, %s, %s) RETURNING contact_id;",
                        (contact_data.get('role'), contact_data.get('email'), contact_data.get('phone'))
                    )
                     contact_id_fk = cursor.fetchone()['contact_id']
            
            # Fallback to a default or error if no contact
            if not contact_id_fk:
                 # Attempt to grab from existing main if rfq_id exists
                 cursor.execute("SELECT contact_id_fk FROM main WHERE rfq_id = %s", (original_rfq_payload.get('rfq_id'),))
                 res_main = cursor.fetchone()
                 if res_main: contact_id_fk = res_main['contact_id_fk']
                 else: contact_id_fk = 1 # DANGEROUS FALLBACK - ideally handle error

            # Map the columns
            columns = [
                'rfq_id', 'customer_name', 'application', 'product_line', 'customer_pn', 'revision_level',
                'delivery_zone', 'delivery_plant', 'sop_year', 'annual_volume', 'rfq_reception_date',
                'quotation_expected_date', 'target_price_eur', 'delivery_conditions', 'payment_terms',
                'business_trigger', 'entry_barriers', 'product_feasibility_note', 'manufacturing_location',
                'risks', 'decision', 'design_responsibility', 'validation_responsibility', 'design_ownership',
                'development_costs', 'technical_capacity', 'scope_alignment', 'overall_feasibility',
                'customer_status', 'strategic_note', 'final_recommendation', 'contact_id_fk', 
                'requester_comment', 'validator_comments', 'status', 'created_by_email', 'validated_by_email', 
                'rfq_file_path', 'update_status'
            ]
            
            def get_val(k): return original_rfq_payload.get(k) if original_rfq_payload.get(k) != '' else None
            def get_bool(k): return convert_to_boolean(original_rfq_payload.get(k))

            values = [
                original_rfq_payload.get('rfq_id'), get_val('customer_name'), get_val('application'), get_val('product_line'), 
                get_val('customer_pn'), get_val('revision_level'),
                get_val('delivery_zone'), get_val('delivery_plant'), 
                get_val('sop_year'), get_val('annual_volume'), 
                get_val('rfq_reception_date'), get_val('quotation_expected_date'), 
                get_val('target_price_eur'), 
                get_val('delivery_conditions'), get_val('payment_terms'), get_val('business_trigger'), 
                get_val('entry_barriers'), get_val('product_feasibility_note'), 
                get_val('manufacturing_location'), get_val('risks'), get_val('decision'), 
                get_val('design_responsibility'), get_val('validation_responsibility'), 
                get_val('design_ownership'), get_val('development_costs'), 
                get_bool('technical_capacity'), get_bool('scope_alignment'), 
                get_val('overall_feasibility'), get_val('customer_status'), get_val('strategic_note'), 
                get_val('final_recommendation'), 
                contact_id_fk, 
                get_val('requester_comment'), 
                comments, # Use the validator's NEW comments here
                get_val('status'), 
                created_by_email, validated_by_email, rfq_file_path, 
                'WAITING_FOR_UPDATE'
            ]

            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO rfq_update_requests ({', '.join(columns)}) VALUES ({placeholders})"
            
            cursor.execute(sql, tuple(values))
            conn.commit()
            
            # Send Email Notification to Creator
            gpt_url = "https://chatgpt.com/g/g-68d8e2cc2cc08191bafeefd60b31cc62-rfq-integration"
            current_rfq_id = original_rfq_payload.get('rfq_id', 'Unknown ID')
            
            msg = Message(f"Action Required: Update Requested for {current_rfq_id}", recipients=[created_by_email])
            msg.html = f"""
            <html>
            <body style="font-family: 'Helvetica', 'Arial', sans-serif; line-height: 1.6; color: #333;">
                <div style="max-width: 600px; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden;">
                    <div style="background-color: #2196F3; padding: 20px; text-align: center;">
                        <h2 style="color: #ffffff; margin: 0;">Update Requested</h2>
                    </div>
                    
                    <div style="padding: 30px;">
                        <p>Hello,</p>
                        <p>The validator has reviewed <strong>{current_rfq_id}</strong> and requested changes before approval.</p>
                        
                        <div style="background-color: #e3f2fd; border-left: 4px solid #2196F3; padding: 15px; margin: 20px 0; border-radius: 4px;">
                            <strong style="color: #1565C0;">Validator Comments:</strong>
                            <p style="margin-top: 5px; font-style: italic;">"{comments}"</p>
                        </div>
                        
                        <p>Please use the link below to return to the AI Assistant. You can paste the RFQ ID to retrieve and modify the data.</p>
                        
                        <div style="text-align: center; margin: 35px 0;">
                            <a href="{gpt_url}" target="_blank" style="background-color: #2196F3; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; font-size: 16px; display: inline-block;">
                                Click Here to Update Your RFQ
                            </a>
                        </div>
                        
                        <p style="font-size: 13px; color: #666; text-align: center;">
                            Ref: <strong>{current_rfq_id}</strong>
                        </p>
                    </div>
                </div>
            </body>
            </html>
            """
            safe_send_mail(msg)

            return f"""<!DOCTYPE html><html lang="en"><head><title>Update Requested</title><style>body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f4; }} .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-left: 6px solid #2196F3; }} h1 {{ color: #2196F3; }}</style></head><body><div class="card"><h1>Update Requested</h1><p>Your comments and the RFQ data have been saved to the update queue.</p></div></body></html>"""

        except Exception as e:
            if conn: conn.rollback()
            return f"Error saving update request: {e}", 500
        finally:
            if conn: conn.close()

    if not isinstance(original_rfq_payload, dict):
        db_submission_error = "FATAL: Original RFQ structured data ('rfq_payload') is missing or invalid."
        db_submission_status = "FAILED"
        payload_to_send = None
    else:
        # Extract emails from the stored request_data
        created_by_email = request_data.get('user_email')
        validated_by_email = request_data.get('validator_email')
        
        payload_to_send = {
            **original_rfq_payload,
            "status": action.upper(),      
            "validator_comments": comments,
            # NEW FIELDS ADDED HERE
            "created_by_email": created_by_email,
            "validated_by_email": validated_by_email,
            "rfq_file_path": rfq_file_path
        }
        db_submission_status = "PENDING"

    # 3. Submit to External Database API
    db_submission_error = ""

    if payload_to_send:
        try:
            print(f"DEBUG: Attempting to submit final RFQ data to {RFQ_SUBMISSION_API_URL}...")
            response = requests.post(
                RFQ_SUBMISSION_API_URL,
                json=payload_to_send, 
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status() 
            db_submission_status = "SUCCESS"
            print(f"DEBUG: External RFQ submission successful. Status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            db_submission_error = str(e)
            db_submission_status = "FAILED"
            print(f"FATAL EXTERNAL SUBMISSION ERROR: {db_submission_error}")

    # 4. Update Monday.com if Confirmed
    monday_update_status = "SKIPPED"
    monday_update_error = ""
    
    if action == 'confirm':
        # We no longer need project_id since we are creating a NEW item
        original_rfq_payload = request_data.get('rfq_payload')
        report_content = request_data.get('report_content')
        
        if original_rfq_payload and report_content:
            # 💡 CALLING THE NEW ITEM CREATION FUNCTION
            monday_ok, monday_result = create_monday_rfq_item(original_rfq_payload, report_content, request_data)
            
            if monday_ok:
                monday_update_status = "SUCCESS"
                # Store the new Monday item ID for logging/tracking if needed
                request_data['monday_item_id'] = monday_result
                set_request_data(request_id, request_data) # Persist the new ID
            else:
                monday_update_status = "FAILED"
                monday_update_error = monday_result
        else:
            monday_update_status = "FAILED"
            monday_update_error = "Missing rfq_payload or report_content in stored data for Monday item creation."

    # 5. Send Final Email to User
    user_email = request_data['user_email']
    color = '#4CAF50' if action == 'confirm' else '#F44336'
    subject = f"✅ Report {action.upper()} - Final Decision" if action == 'confirm' else f"❌ Report {action.upper()} - Final Decision"

    final_email_body_html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height: 1.6;"><h2>Your AI RFQ Validation Result</h2><p>The validator, <strong>{request_data['validator_email']}</strong>, has finished reviewing your report.</p><hr>
            <h3 style="color: {color};">Decision: {action.upper()}</h3><h4>Validator Comments:</h4><div style="border-left: 5px solid {color}; padding: 10px; background-color: #e9f5ff; border-radius: 0 4px 4px 0; margin-bottom: 20px;"><p style="margin: 0;">{comments}</p></div>
            <h4>Database Submission Status: <span style="color: {'#4CAF50' if db_submission_status == 'SUCCESS' else '#F44336'};">{db_submission_status}</span></h4>
            {f'<p style="color: #F44336;">External RFQ Error: {db_submission_error}</p>' if db_submission_status != 'SUCCESS' else ''}
            
            <h4>Monday.com Update Status (Confirmation Only): <span style="color: {'#4CAF50' if monday_update_status == 'SUCCESS' else ('#FFC107' if monday_update_status == 'SKIPPED' else '#F44336')};">{monday_update_status}</span></h4>
            {f'<p style="color: #F44336;">Monday.com Error: {monday_update_error}</p>' if monday_update_status == 'FAILED' else ''}

            <p style="margin-top: 20px;">Thank you for using the AI Assistant.</p></body></html>
    """
    msg = Message(subject, recipients=[user_email])
    msg.html = final_email_body_html
    safe_send_mail(msg) 

    # 6. Show Success/Failure Page
    if db_submission_status != 'SUCCESS' or monday_update_status == 'FAILED':
        overall_error_details = (f"External RFQ Submission Error: {db_submission_error}" if db_submission_status != 'SUCCESS' else "")
        overall_error_details += (f"\nMonday.com Update Error: {monday_update_error}" if monday_update_status == 'FAILED' else "")
        
        return f"""<!DOCTYPE html><html lang="en"><head><title>Submission Failed</title><style>body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #fef2f2; }} .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-left: 6px solid #dc2626; }} h1 {{ color: #dc2626; margin-bottom: 6px; }} pre {{ text-align: left; white-space: pre-wrap; background:#fff7ed; padding:12px; border-radius:6px; border:1px solid #fed7aa }}</style></head>
        <body><div class="card"><h1>Decision Saved, External Submission Failed</h1><p>Your validation decision was saved, but one or more external updates failed (RFQ API: {db_submission_status}, Monday: {monday_update_status}).</p><h3 style="margin-top:18px;">Error Details</h3><pre>{overall_error_details}</pre><p>The user was notified of this failure via email.</p></div></body></html>""", 500

    # Successful completion page
    return f"""<!DOCTYPE html><html lang="en"><head><title>Success</title><style>body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f4; }} .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-left: 6px solid {color}; }} h1 {{ color: {color}; }} p {{ color: #4b5563; }} .pill {{ display:inline-block; padding: 6px 10px; border-radius: 9999px; background:#ecfdf5; color:#065f46; font-weight:600; }}</style></head>
    <body><div class="card"><h1>Validation Complete!</h1><p class="pill">Decision: {action.upper()}</p><p style="margin-top:14px;">Your decision has been recorded, the final RFQ data was successfully submitted to the database, and the Monday.com board was updated (Status: {monday_update_status}).</p><p style="margin-top:18px;"><a href="/" style="color:#2563eb; text-decoration:none;">Return to start</a></p></div></body></html>"""


# ======================== APOLLO ROUTES ========================
# ======================== APOLLO ROUTES ========================

# ======================== MODIFIED APOLLO ROUTES ========================

@app.route('/apollo/search', methods=['POST'])
@app.route('/api/v1/mixed_people/search', methods=['POST'])
def search_people_simple():
    """
    Search for people across multiple organizations.
    Returns filtered contact data: name, title, email, linkedin_url, organization_name
    """
    data = request.get_json()
    x_api_key = request.headers.get('X-Api-Key')
    
    # Validate request
    is_valid, error_msg, req = validate_search_request(data)
    if not is_valid:
        return jsonify({"status": "error", "message": error_msg}), 400

    try:
        client = get_apollo_client(x_api_key)
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    all_contacts: List[Dict[str, Any]] = []
    total_entries = 0
    organizations_searched: List[str] = []
    errors: List[Dict[str, str]] = []

    for idx, org_name in enumerate(req["q_organization_name"]):
        try:
            payload: Dict[str, Any] = {
                "q_organization_name": org_name,
                "page": req["page"],
                "per_page": req["per_page"]
            }
            if req["person_titles"]:
                payload["person_titles"] = req["person_titles"]
            if req["person_seniorities"]:
                payload["person_seniorities"] = req["person_seniorities"]
            if req["organization_num_employees_ranges"]:
                payload["organization_num_employees_ranges"] = req["organization_num_employees_ranges"]
            if req["q_organization_domains"]:
                payload["q_organization_domains"] = req["q_organization_domains"]

            response_data = client.search_single_organization(payload)

            contacts = response_data.get("contacts", []) or response_data.get("people", [])
            
            # ✅ FILTER CONTACTS HERE
            filtered_contacts = [filter_search_contact(contact) for contact in contacts]
            all_contacts.extend(filtered_contacts)

            pagination = response_data.get("pagination", {})
            total_entries += pagination.get("total_entries", 0)
            organizations_searched.append(org_name)

            print(f"✓ Found {len(filtered_contacts)} contacts from {org_name}")

            if idx < len(req["q_organization_name"]) - 1:
                time.sleep(req["delay_between_requests"])

        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", "unknown")
            errors.append({"organization": org_name, "error": f"HTTP error searching {org_name}: {status}"})
            print(f"✗ HTTP error searching {org_name}: {status}")
        except Exception as e:
            errors.append({"organization": org_name, "error": f"Error searching {org_name}: {str(e)}"})
            print(f"✗ Error searching {org_name}: {str(e)}")

    response: Dict[str, Any] = {
        "organizations_searched": organizations_searched,
        "total_organizations": len(req["q_organization_name"]),
        "successful_searches": len(organizations_searched),
        "failed_searches": len(errors),
        "total_contacts": len(all_contacts),
        "contacts": all_contacts,
        "pagination": {
            "page": req["page"],
            "per_page": req["per_page"],
            "total_entries": total_entries
        }
    }
    if errors:
        response["errors"] = errors
    
    return jsonify(response), 200


@app.route('/apollo/enrich', methods=['POST'])
@app.route('/api/v1/people/match', methods=['POST'])
def enrich_person():
    """
    Enrich a single person.
    Returns filtered data: first_name, last_name, name, title, email, linkedin_url, phone_numbers, organization.name
    """
    data = request.get_json()
    x_api_key = request.headers.get('X-Api-Key')
    
    # Validate request
    is_valid, error_msg, req = validate_enrich_request(data)
    if not is_valid:
        return jsonify({"status": "error", "message": error_msg}), 400

    # Validation
    if req["reveal_phone_number"] and not req["webhook_url"]:
        return jsonify({
            "status": "error",
            "message": "webhook_url is mandatory when reveal_phone_number is True"
        }), 400

    payload: Dict[str, Any] = {}
    if req["name"]:
        payload["name"] = req["name"]
    else:
        if req["first_name"]:
            payload["first_name"] = req["first_name"]
        if req["last_name"]:
            payload["last_name"] = req["last_name"]

    if req["organization_name"]:
        payload["organization_name"] = req["organization_name"]
    if req["domain"]:
        payload["domain"] = req["domain"]
    if req["email"]:
        payload["email"] = req["email"]
    if req["id"]:
        payload["id"] = req["id"]
    if req["linkedin_url"]:
        payload["linkedin_url"] = req["linkedin_url"]
    if req["reveal_personal_emails"]:
        payload["reveal_personal_emails"] = True
    if req["reveal_phone_number"]:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = req["webhook_url"]

    try:
        client = get_apollo_client(x_api_key)
        response_data = client.enrich_person(payload)
        
        # ✅ FILTER RESPONSE HERE
        filtered_response = filter_enrich_contact(response_data)
        
        return jsonify(filtered_response), 200
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"status": "error", "message": text}), status
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# --- Route mise à jour : /apollo/bulk_enrich ---
@app.route('/apollo/bulk_enrich', methods=['POST'])
def bulk_enrich_people():
    # Option d’override de la clé via l’en-tête (facultatif)
    x_api_key = request.headers.get('X-Api-Key')

    # Sécurité du contenu
    if not request.is_json:
        return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 415

    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"status": "error", "message": "Invalid JSON body"}), 400

    # Règle métier : si on veut révéler les numéros, le webhook est requis
    reveal_phone = bool(payload.get("reveal_phone_number"))
    webhook_url = payload.get("webhook_url")
    if reveal_phone and not webhook_url:
        return jsonify({
            "status": "error",
            "message": "webhook_url is required when reveal_phone_number=true"
        }), 400

    try:
        client = get_apollo_client(x_api_key)
        response_data = client.bulk_enrich(payload)

        # matches peut contenir des dicts ET/OU des None -> filtrage tolérant
        matches = response_data.get("matches") or []

        filtered_matches = []
        for m in matches:
            filtered_matches.append(filter_enrich_contact_bulk(m))

        # Aligner les compteurs avec ceux renvoyés par Apollo (ton exemple de réponse)
        filtered_response = {
            "matches": filtered_matches,
            "total_requested_enrichments": response_data.get("total_requested_enrichments"),
            "unique_enriched_records": response_data.get("unique_enriched_records"),
            "missing_records": response_data.get("missing_records"),
            "credits_consumed": response_data.get("credits_consumed"),
        }

        return jsonify(filtered_response), 200

    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"status": "error", "message": text}), status
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ======================== RFQ API ENDPOINTS ========================

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

    # Extract validation fields if present
    final_status = data.pop('status', None) 
    final_validator_comments = data.pop('validator_comments', None)
    created_by_email = data.pop('created_by_email', None)
    validated_by_email = data.pop('validated_by_email', None)
    rfq_file_path = data.pop('rfq_file_path', None)
    # Normalize status from list to string if needed
    if isinstance(final_status, list) and final_status:
        final_status = final_status[0]

    def get_numeric(key):
        val = data.get(key)
        return val if val != '' else None

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

        data.pop('contact', None)

        # Build column names and values including validation fields
        COLUMN_NAMES = [
            'rfq_id', 'customer_name', 'application', 'product_line', 'customer_pn', 'revision_level',
            'delivery_zone', 'delivery_plant', 'sop_year', 'annual_volume', 'rfq_reception_date',
            'quotation_expected_date', 'target_price_eur', 'delivery_conditions', 'payment_terms',
            'business_trigger', 'entry_barriers', 'product_feasibility_note', 'manufacturing_location',
            'risks', 'decision', 'design_responsibility', 'validation_responsibility', 'design_ownership',
            'development_costs', 'technical_capacity', 'scope_alignment', 'overall_feasibility',
            'customer_status', 'strategic_note', 'final_recommendation', 'contact_id_fk', 
            'requester_comment', # <--- NEW
            'validator_comments', 'status','created_by_email', 'validated_by_email','rfq_file_path'
        ]

        main_values = [
            data.get('rfq_id'), data.get('customer_name'), data.get('application'), data.get('product_line'), 
            data.get('customer_pn'), data.get('revision_level'),
            data.get('delivery_zone'), data.get('delivery_plant'), 
            get_numeric('sop_year'), get_numeric('annual_volume'), 
            data.get('rfq_reception_date'), data.get('quotation_expected_date'), 
            get_numeric('target_price_eur'), 
            data.get('delivery_conditions'), data.get('payment_terms'), data.get('business_trigger'), 
            data.get('entry_barriers'), data.get('product_feasibility_note'), 
            data.get('manufacturing_location'), data.get('risks'), data.get('decision'), 
            data.get('design_responsibility'), data.get('validation_responsibility'), 
            data.get('design_ownership'), data.get('development_costs'), 
            convert_to_boolean(data.get('technical_capacity')), convert_to_boolean(data.get('scope_alignment')), 
            data.get('overall_feasibility'), data.get('customer_status'), data.get('strategic_note'), 
            data.get('final_recommendation'), 
            contact_id_fk, 
            data.get('requester_comment'), # <--- NEW
            final_validator_comments, final_status,created_by_email, validated_by_email,rfq_file_path
        ]

        columns_sql = ', '.join(COLUMN_NAMES)
        placeholders_sql = ', '.join(['%s'] * len(COLUMN_NAMES))

        insert_main_sql = f"INSERT INTO main ({columns_sql}) VALUES ({placeholders_sql})"

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
        conn = psycopg2.connect(**RFQ_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
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
        conn = psycopg2.connect(**RFQ_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

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
        conn = psycopg2.connect(**RFQ_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

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
        conn = psycopg2.connect(**RFQ_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

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


@app.route('/api/rfq/max-id', methods=['GET'])
def get_max_rfq_id():
    """
    Retrieves the maximum rfq_id from the 'main' table.
    This helps in generating the next sequential RFQ ID.
    """
    conn = None
    try:
        conn, cursor = get_db()

        # Assuming rfq_id is a unique identifier (like an integer or serial number)
        # If rfq_id is a string, you might need to cast it or adjust the query based on your ID format.
        # This query assumes it can be max()'d directly, which works for sequential number formats.
        query = "SELECT MAX(id) AS max_rfq_id FROM main;"
        cursor.execute(query)

        result = cursor.fetchone()
        max_id = result['max_rfq_id'] if result and 'max_rfq_id' in result else None

        if max_id is None:
            # If the table is empty, return a starting value, e.g., 0
            # or the first actual ID to be generated (e.g., '1000' if using a prefix).
            # We'll return 0 here, assuming IDs are numbers or strings that sort correctly.
            return jsonify({
                "status": "success",
                "message": "Table is empty, returning initial ID.",
                "max_rfq_id": 0
            }), 200

        return jsonify({
            "status": "success",
            "message": "Successfully retrieved maximum RFQ ID.",
            "max_rfq_id": max_id
        }), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "status": "error",
            "message": f"Database query failed: {pg_error_message}"
        }), 500

    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500

    finally:
        if conn:
            conn.close()


@app.route('/api/contact/check', methods=['GET'])
def check_contact_existence():
    """
    Checks the existence of a contact in the database using their email.
    If found, returns the contact's role and phone number.
    Example: GET /api/contact/check?email=buyer@example.com
    """
    email = request.args.get('email')

    if not email:
        return jsonify({
            "status": "error",
            "message": "Missing required query parameter: email."
        }), 400

    conn = None
    try:
        conn, cursor = get_db()
        
        # NOTE: Using ILIKE for case-insensitive matching
        query = """
            SELECT contact_role, contact_phone
            FROM contact
            WHERE contact_email ILIKE %s;
        """
        cursor.execute(query, (email,))
        result = cursor.fetchone()

        if result:
            return jsonify({
                "status": "success",
                "message": "Contact found.",
                "contact_exists": True,
                "contact_role": result['contact_role'],
                "contact_phone": result['contact_phone']
            }), 200
        else:
            return jsonify({
                "status": "success",
                "message": "Contact not found.",
                "contact_exists": False,
                "contact_role": None,
                "contact_phone": None
            }), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "status": "error",
            "message": f"Database query failed: {pg_error_message}"
        }), 500

    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if conn:
            conn.close()




@app.route('/api/data/ingest', methods=['POST'])
def data_ingestion():
    """
    Ingests new Customer (Groupe), Plant (Unit), and Contact (Person) data 
    from a generated RFQ payload if the records do not already exist.
    
    This endpoint uses CLIENT_DB connection (not RFQ_DATA).
    
    Expected additional fields for the assistant user:
    - assistant_user_email
    - assistant_user_first_name
    - assistant_user_last_name
    - assistant_user_job_title
    """
    data = request.get_json()
    conn = None

    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    # Required fields check
    required_keys = [
        'customer_name', 'delivery_zone', 'plant_name', 'city', 'country', 
        'contact_email', 'contact_first_name', 'contact_last_name', 
        'contact_job_title', 'contact_phone', 'contact_role', # RFQ contact info
        'assistant_user_email', 'assistant_user_first_name', 'assistant_user_last_name', 'assistant_user_job_title' # Assistant info
    ]
    if not all(key in data and data[key] is not None for key in required_keys):
        missing = [key for key in required_keys if key not in data or data[key] is None]
        return jsonify({"status": "error", "message": f"Missing or null required fields: {', '.join(missing)}"}), 400

    # Extracting data
    customer_name = data['customer_name']
    delivery_zone = data['delivery_zone']
    plant_name = data['plant_name']
    city = data['city']
    country = data['country']
    
    # RFQ Contact data
    contact_email = data['contact_email']
    contact_first_name = data['contact_first_name']
    contact_last_name = data['contact_last_name']
    contact_job_title = data['contact_job_title']
    contact_phone = data['contact_phone']
    contact_role = data['contact_role']

    # Assistant/Commercial data
    assistant_email = data['assistant_user_email']
    assistant_first_name = data['assistant_user_first_name']
    assistant_last_name = data['assistant_user_last_name']
    assistant_job_title = data['assistant_user_job_title'] # Used as Commercial's job title

    try:
        # IMPORTANT: Connect to Client_DB instead of RFQ_DATA
        conn = psycopg2.connect(**CLIENT_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- 0. Assistant/Commercial Person Ingestion ---
        # The assistant user is the Commercial Person for the unit. Role is set to 'Commercial'.
        assistant_person_id = None
        cursor.execute('SELECT "Person_id" FROM public."Person" WHERE email = %s;', (assistant_email,))
        existing_assistant = cursor.fetchone()
        
        if existing_assistant:
            assistant_person_id = existing_assistant['Person_id']
            assistant_status = "EXISTING"
        else:
            # Note: Preserved quoted "role" column name.
            insert_assistant_sql = '''
                INSERT INTO public."Person" (first_name, last_name, job_title, email, phone_number, "role", zone_name) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING "Person_id";
            '''
            cursor.execute(insert_assistant_sql, (
                assistant_first_name, assistant_last_name, assistant_job_title, 
                assistant_email, None, 'Commercial', delivery_zone
            ))
            result = cursor.fetchone()
            if not result:
                raise Exception("Failed to create new assistant person, no ID returned.")
            assistant_person_id = result['Person_id']
            assistant_status = "NEW"

        # --- 1. Groupe/Customer Ingestion ---
        groupe_id = None
        cursor.execute("SELECT groupe_id FROM public.groupe WHERE groupe_name = %s;", (customer_name,))
        existing_groupe = cursor.fetchone()
        
        if existing_groupe:
            groupe_id = existing_groupe['groupe_id']
            groupe_status = "EXISTING"
        else:
            insert_groupe_sql = """
                INSERT INTO public.groupe (groupe_name) 
                VALUES (%s) 
                RETURNING groupe_id;
            """
            cursor.execute(insert_groupe_sql, (customer_name,))
            result = cursor.fetchone()
            if not result:
                raise Exception("Failed to create new groupe, no ID returned.")
            groupe_id = result['groupe_id']
            groupe_status = "NEW"

        # --- 2. Unit/Plant Ingestion ---
        unit_id = None
        # Check existence based on unit_name (plant_name) and group
        cursor.execute("SELECT unit_id FROM public.unit WHERE unit_name = %s AND groupe_id = %s;", (plant_name, groupe_id))
        existing_unit = cursor.fetchone()

        if existing_unit:
            unit_id = existing_unit['unit_id']
            unit_status = "EXISTING"
            
            # If the unit already exists, update the com_person_id 
            update_unit_sql = "UPDATE public.unit SET com_person_id = %s WHERE unit_id = %s;"
            cursor.execute(update_unit_sql, (assistant_person_id, unit_id))
        else:
            insert_unit_sql = """
                INSERT INTO public.unit (groupe_id, unit_name, city, country, zone_name, com_person_id) 
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING unit_id;
            """
            cursor.execute(insert_unit_sql, (
                groupe_id, plant_name, city, country, delivery_zone, assistant_person_id
            ))
            result = cursor.fetchone()
            if not result:
                raise Exception("Failed to create new unit, no ID returned.")
            unit_id = result['unit_id']
            unit_status = "NEW"
            
        # --- 3. RFQ Contact Person Ingestion (Must be separate from Assistant Person) ---
        person_id = None
        cursor.execute('SELECT "Person_id" FROM public."Person" WHERE email = %s;', (contact_email,))
        existing_person = cursor.fetchone()
        
        if existing_person:
            person_id = existing_person['Person_id']
            person_status = "EXISTING"
        else:
            # Note: factory_id in Person table maps to unit_id
            insert_person_sql = '''
                INSERT INTO public."Person" (factory_id, first_name, last_name, job_title, email, phone_number, "role", zone_name) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING "Person_id";
            '''
            cursor.execute(insert_person_sql, (
                unit_id, contact_first_name, contact_last_name, 
                contact_job_title, contact_email, contact_phone, contact_role, delivery_zone
            ))
            result = cursor.fetchone()
            if not result:
                raise Exception("Failed to create new contact person, no ID returned.")
            person_id = result['Person_id']
            person_status = "NEW"

        conn.commit()

        return jsonify({
            "status": "success",
            "message": "Data ingestion complete. Records inserted only if new.",
            "groupe": {"status": groupe_status, "id": groupe_id, "name": customer_name},
            "unit": {"status": unit_status, "id": unit_id, "name": plant_name, "com_person_id": assistant_person_id},
            "contact_person": {"status": person_status, "id": person_id, "email": contact_email},
            "commercial_person": {"status": assistant_status, "id": assistant_person_id, "email": assistant_email}
        }), 200

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
            
        pg_error_message = getattr(e, 'pgerror', None)
        detail_message = pg_error_message.strip() if pg_error_message else str(e)

        return jsonify({
            "status": "error",
            "message": f"Database ingestion failed (PostgreSQL Error): {detail_message}"
        }), 500

    except Exception as e:
        if conn:
            conn.rollback()
            
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if conn:
            cursor.close()
            conn.close()
            





@app.route('/api/data/groupe/check', methods=['GET'])
def check_groupe_existence():
    """
    Checks the existence of a customer (Groupe) in the Client_DB database
    using their name. If found, returns the groupe_id.
    Example: GET /api/data/groupe/check?groupeName=BMW
    """
    groupe_name = request.args.get('groupeName')

    if not groupe_name:
        return jsonify({
            "status": "error",
            "message": "Missing required query parameter: groupeName."
        }), 400

    conn = None
    try:
        # IMPORTANT: Connect to CLIENT_DB
        conn = psycopg2.connect(**CLIENT_DB_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # NOTE: Using ILIKE for case-insensitive matching
        query = """
            SELECT groupe_id, groupe_name
            FROM public.groupe
            WHERE groupe_name ILIKE %s;
        """
        cursor.execute(query, (groupe_name,))
        result = cursor.fetchone()

        if result:
            return jsonify({
                "status": "success",
                "message": "Groupe found.",
                "groupe_exists": True,
                "groupe_id": result['groupe_id'],
                "groupe_name_db": result['groupe_name'] # Return DB name for confirmation
            }), 200
        else:
            return jsonify({
                "status": "success",
                "message": "Groupe not found.",
                "groupe_exists": False,
                "groupe_id": None
            }), 200

    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({
            "status": "error",
            "message": f"Database query failed: {pg_error_message}"
        }), 500

    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()


# ======================== FILE UPLOAD ENDPOINT ========================







@app.route('/api/upload-file', methods=['POST'])
def upload_file():
    # 1. Get the list of file references
    data = request.get_json(silent=True) or {}
    refs = data.get('openaiFileIdRefs', [])
    
    if not refs:
        return jsonify({
            "message": "No openaiFileIdRefs in JSON request",
            "received_content_type": request.content_type
        }), 400
    
    # 2. Load Configuration
    token = os.environ.get('GITHUB_TOKEN')
    repo_full_name = "STS-Engineer/RFQ-back"
    branch = "main"

    if not token or not repo_full_name:
        app.logger.error("GitHub configuration (GITHUB_TOKEN, GITHUB_REPO) is missing.")
        return jsonify({"message": "Server configuration error: GitHub token/repo not set"}), 500

    uploaded_paths = []
    errors = []

    # 3. Loop through ALL files in the request
    for file_ref in refs:
        try:
            # Handle dictionary vs string format
            if isinstance(file_ref, dict):
                download_link = file_ref.get('download_link')
                original_name = file_ref.get('name') or 'uploaded_file'
            else:
                download_link = file_ref
                original_name = 'uploaded_file'

            if not download_link:
                continue

            # A. Download content
            app.logger.info(f"Downloading file: {original_name}")
            r = requests.get(download_link, stream=False, timeout=10)
            r.raise_for_status()
            file_content_bytes = r.content

            # B. Secure Filename
            filename_safe = secure_filename(original_name)
            ext = filename_safe.rsplit('.', 1)[1].lower() if '.' in filename_safe else 'bin'
            
            if not allowed_file(f'dummy.{ext}'):
                errors.append(f"{original_name}: File type not allowed")
                continue

            # C. Prepare for GitHub
            unique_filename = f"rfq_upload_{uuid.uuid4().hex[:8]}_{int(time.time())}.{ext}"
            file_path_in_repo = f"uploads/{unique_filename}"
            
            content_b64 = base64.b64encode(file_content_bytes).decode('utf-8')
            
            api_url = f"https://api.github.com/repos/{repo_full_name}/contents/{file_path_in_repo}"
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
            }
            payload = {
                "message": f"Upload RFQ file: {unique_filename}",
                "content": content_b64,
                "branch": branch
            }
            
            # D. Upload
            put_response = requests.put(api_url, headers=headers, json=payload, timeout=20)
            put_response.raise_for_status()
            
            # E. Collect Success Path
            # We return the path starting with /uploads/
            needed_path = '/' + file_path_in_repo
            uploaded_paths.append(needed_path)

        except Exception as e:
            app.logger.error(f"Failed to upload {original_name}: {e}")
            errors.append(f"{original_name}: {str(e)}")

    # 4. Return result
    if not uploaded_paths and errors:
        return jsonify({"message": "All uploads failed", "errors": errors}), 500

    return jsonify({
        "status": "success",
        "message": f"Uploaded {len(uploaded_paths)} files.",
        "file_paths": uploaded_paths,  # <--- Now returning an ARRAY
        "errors": errors
    }), 200
  



@app.route('/api/rfq/update/<string:rfq_id>', methods=['POST', 'PUT'])
def update_rfq(rfq_id):
    """
    Updates an existing RFQ record in the 'main' table using its rfq_id.
    This expects the FULL RFQ object, similar to the /submit endpoint.
    """
    data = request.get_json()
    conn = None
    
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    try:
        conn, cursor = get_db()
        
        # --- 1. Check if RFQ exists and get original contact_id_fk ---
        cursor.execute("SELECT contact_id_fk FROM main WHERE rfq_id = %s", (rfq_id,))
        existing_rfq = cursor.fetchone()
        
        if not existing_rfq:
            return jsonify({"status": "error", "message": f"RFQ ID {rfq_id} not found."}), 404
        
        original_contact_id_fk = existing_rfq['contact_id_fk']

        # --- 2. Handle Contact (Get or Create) ---
        contact_id_fk = None
        contact_data = data.pop('contact', {})
        contact_email = contact_data.get('email')

        if contact_email:
            # If contact email is provided, get or create new contact
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
        else:
            # If no contact info provided in update, re-use the original contact ID
            contact_id_fk = original_contact_id_fk

        # --- 3. Extract validation/special fields ---
        # (Using data.get() allows these fields to be in the payload or not)
        final_status = data.get('status')
        final_validator_comments = data.get('validator_comments')
        created_by_email = data.get('created_by_email')
        validated_by_email = data.get('validated_by_email')
        rfq_file_path = data.get('rfq_file_path')
        # Note: 'updated_by' will be fetched directly in the values list below

        if isinstance(final_status, list) and final_status:
            final_status = final_status[0]
            
        def get_numeric(key):
            val = data.get(key)
            return val if val != '' else None

        # --- 4. Build column list and value list (must match /submit) ---
        COLUMN_NAMES = [
            'customer_name', 'application', 'product_line', 'customer_pn', 'revision_level',
            'delivery_zone', 'delivery_plant', 'sop_year', 'annual_volume', 'rfq_reception_date',
            'quotation_expected_date', 'target_price_eur', 'delivery_conditions', 'payment_terms',
            'business_trigger', 'entry_barriers', 'product_feasibility_note', 'manufacturing_location',
            'risks', 'decision', 'design_responsibility', 'validation_responsibility', 'design_ownership',
            'development_costs', 'technical_capacity', 'scope_alignment', 'overall_feasibility',
            'customer_status', 'strategic_note', 'final_recommendation', 'contact_id_fk', 
            'requester_comment', # <--- NEW
            'validator_comments', 'status', 'created_by_email', 'validated_by_email', 'rfq_file_path',
            'updated_by' 
        ]

        main_values = [
            data.get('customer_name'), data.get('application'), data.get('product_line'), 
            data.get('customer_pn'), data.get('revision_level'),
            data.get('delivery_zone'), data.get('delivery_plant'), 
            get_numeric('sop_year'), get_numeric('annual_volume'), 
            data.get('rfq_reception_date'), data.get('quotation_expected_date'), 
            get_numeric('target_price_eur'), 
            data.get('delivery_conditions'), data.get('payment_terms'), data.get('business_trigger'), 
            data.get('entry_barriers'), data.get('product_feasibility_note'), 
            data.get('manufacturing_location'), data.get('risks'), data.get('decision'), 
            data.get('design_responsibility'), data.get('validation_responsibility'), 
            data.get('design_ownership'), data.get('development_costs'), 
            convert_to_boolean(data.get('technical_capacity')), convert_to_boolean(data.get('scope_alignment')), 
            data.get('overall_feasibility'), data.get('customer_status'), data.get('strategic_note'), 
            data.get('final_recommendation'), 
            contact_id_fk, 
            data.get('requester_comment'), # <--- NEW
            final_validator_comments, final_status, created_by_email, validated_by_email, rfq_file_path,
            data.get('updated_by') 
        ]

        # --- 5. Construct and Execute UPDATE statement ---
        set_clause = ", ".join([f"{col} = %s" for col in COLUMN_NAMES])
        
        update_main_sql = f"UPDATE main SET {set_clause} WHERE rfq_id = %s"

        # Add the rfq_id to the end of the values list for the WHERE clause
        query_values = main_values + [rfq_id]
        
        cursor.execute(update_main_sql, tuple(query_values))
        
        conn.commit()

        return jsonify({
            "status": "success", 
            "message": "RFQ data successfully updated.", 
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
            "message": f"Database update failed (PostgreSQL Error): {detail_message}",
            "rfq_id": rfq_id
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



@app.route('/api/rfq/update-request/<string:rfq_id>', methods=['GET'])
def get_rfq_update_request_data(rfq_id):
    """
    Retrieves data from rfq_update_requests table for a specific RFQ ID.
    Returns a list of requests sorted by creation date (newest first).
    """
    conn = None
    try:
        conn, cursor = get_db()

        # Query to fetch all columns
        query = """
            SELECT * FROM public.rfq_update_requests 
            WHERE rfq_id = %s 
            ORDER BY created_at DESC
        """
        
        cursor.execute(query, (rfq_id,))
        results = cursor.fetchall()

        if not results:
            return jsonify({
                "status": "success",
                "message": f"No update requests found for RFQ ID: {rfq_id}",
                "data": []
            }), 200

        # Helper function to serialize SQL types (DateTime, Decimal) to JSON-friendly formats
        def serialize_row(row):
            clean_row = dict(row)
            for key, value in clean_row.items():
                # Handle Timestamps and Dates
                if isinstance(value, (datetime.date, datetime.datetime)):
                    clean_row[key] = value.isoformat()
                # Handle Numeric/Decimal types
                elif isinstance(value, Decimal):
                    clean_row[key] = float(value)
            return clean_row

        # Apply serialization to all fetched rows
        formatted_data = [serialize_row(row) for row in results]

        return jsonify({
            "status": "success",
            "count": len(formatted_data),
            "data": formatted_data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": f"An error occurred retrieving update requests: {str(e)}"
        }), 500
        
    finally:
        if conn:
            conn.close()





if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
