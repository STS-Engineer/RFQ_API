import os
import time
import uuid
import datetime
import sys
import json
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2 import OperationalError, errorcodes, extras, RealDictCursor
import requests
from flask import Flask, request, jsonify
from flask_mail import Mail, Message

# ------------------------ Environment Variables ------------------------
# Apollo API Key - hardcoded for simplicity
ENV_API_KEY = "WsqN_6KDO9kkRBt1ZvkDDw"
ALLOW_HEADER_OVERRIDE = os.environ.get("ALLOW_HEADER_OVERRIDE", "true").lower() in ("1", "true", "yes")

# --- 1. CONFIGURATION ---
# PostgreSQL configuration
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
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
def update_monday_rfq_report(project_id, report_content):
    """
    Submits the validated report content to the specified Monday.com item.
    FIXED: Ensures report_content is correctly JSON-escaped for the GraphQL mutation.
    """
    if not MONDAY_API_TOKEN or MONDAY_API_TOKEN == "YOUR_MONDAY_API_TOKEN":
        print("FATAL: Monday API Token is not configured.")
        return False, "Monday API Token not configured."
        
    item_id = project_id
    board_id = "9550168457"  # Replace with your board ID
    column_id = "long_text_mkwh4mee" # Replace with your target column ID

   # 1. Prepare the column value in the structure Monday.com expects for Long Text
    # It must be a JSON object with a 'text' key.
    monday_column_object = {
        "text": report_content
    }

    # 2. JSON-encode the entire object for safe transmission as the GraphQL 'value' argument
    column_value_json = json.dumps(monday_column_object) 

    # 3. Construct the GraphQL mutation.
    # The 'value' argument is where we inject the double-encoded string.
    mutation = f"""
        mutation {{
            change_column_value(
                board_id: "{board_id}",  
                item_id: "{item_id}",    
                column_id: "{column_id}", 
                value: {json.dumps(column_value_json)} 
            ) {{
                id
            }}
        }}
    """
    # NOTE: The outer json.dumps(column_value_json) ensures the inner JSON string 
    # is safely embedded as a string literal within the GraphQL JSON payload.
    
    headers = {
        "Authorization": MONDAY_API_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        print(f"DEBUG: Attempting to update Monday.com item {item_id} with report content...")
        response = requests.post(
            MONDAY_API_URL,
            json={'query': mutation},
            headers=headers
        )
        response.raise_for_status()
        
        response_data = response.json()
        if 'errors' in response_data:
            return False, f"GraphQL Error: {response_data['errors']}"
            
        print(f"DEBUG: Monday.com update successful for item {item_id}.")
        return True, None
        
    except requests.exceptions.RequestException as e:
        error_message = f"Monday.com API Request Failed: {e}"
        print(f"FATAL MONDAY SUBMISSION ERROR: {error_message}")
        return False, error_message
    except Exception as e:
        error_message = f"Unexpected Error during Monday update: {e}"
        print(f"FATAL MONDAY SUBMISSION ERROR: {error_message}")
        return False, error_message

# ------------------------ Root Routes ------------------------
@app.route('/')
def root():
    return {
        "status": "online",
        "service": "Merged RFQ & Apollo.io API with Validation Workflow",
        "version": "2.0.0",
        "endpoints": {
            "rfq_submit": "/api/rfq/submit",
            "rfq_get": "/api/rfq/get",
            "products": "/api/products",
            "product_lines": "/api/product-lines",
            "product_lines_list": "/api/product-lines/list",
            "product_lines_details": "/api/product-lines/details",
            "apollo_search": "/apollo/search",
            "apollo_enrich": "/apollo/enrich",
            "apollo_bulk_enrich": "/apollo/bulk_enrich",
            "validation_send_report": "/api/send-report",
            "validation_page": "/validate-page",
            "validation_handle": "/api/handle-validation"
        }
    }

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
    project_id = data.get('project_id')

    if not all([report_content, user_email, validator_email, rfq_payload, project_id]):
        print("DEBUG: Missing required data.")
        return jsonify({"message": "Missing required data (report, user_email, validator_email, rfq_payload, project_id)"}), 400

    # 2. Store State and Generate Token
    request_id = str(uuid.uuid4())
    request_data = {
        'report_content': report_content,
        'user_email': user_email,
        'validator_email': validator_email,
        'rfq_payload': rfq_payload,
        'project_id': project_id, 
        'status': 'PENDING',
        'validator_comments': None,
        'created_at': datetime.datetime.now().isoformat()
    }
    set_request_data(request_id, request_data)

    # 3. Create Interactive Links
    confirm_link = f"{BASE_URL}/validate-page?id={request_id}&action=confirm"
    decline_link = f"{BASE_URL}/validate-page?id={request_id}&action=decline"

    # 4. Construct Email
    email_body_html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height: 1.6;"><h2>Action Required: AI Report Validation</h2><p>A report generated by the AI assistant requires your review and validation. Please click one of the buttons below to proceed to the confirmation page and add comments.</p><hr><div style="padding: 15px; border: 1px solid #ccc; background-color: #f9f9f9; border-radius: 4px;"><strong>Report Content:</strong><pre style="white-space: pre-wrap; font-family: monospace; font-size: 14px; margin: 10px 0;">{report_content}</pre></div><hr>
            <table width="100%" border="0" cellspacing="0" cellpadding="0"><tr><td><table border="0" cellspacing="0" cellpadding="0" style="margin: 20px 0;"><tr>
                                <td align="center" style="border-radius: 4px;" bgcolor="#4CAF50"><a href="{confirm_link}" target="_blank" style="font-size: 16px; font-weight: bold; font-family: Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; padding: 15px 25px; border-radius: 4px; border: 1px solid #4CAF50; display: inline-block;">CONFIRM REPORT</a></td>
                                <td width="20"></td>
                                <td align="center" style="border-radius: 4px;" bgcolor="#F44336"><a href="{decline_link}" target="_blank" style="font-size: 16px; font-weight: bold; font-family: Helvetica, Arial, sans-serif; color: #ffffff; text-decoration: none; padding: 15px 25px; border-radius: 4px; border: 1px solid #F44336; display: inline-block;">DECLINE REPORT</a></td>
                            </tr></table></td></tr></table>
            <p><small>Initiated by: {user_email}</small></p></body></html>"""

    # 5. Send Email
    msg = Message("AI Report Validation Required", recipients=[validator_email])
    msg.html = email_body_html
    ok, err = safe_send_mail(msg)

    if not ok:
        return jsonify({"message": "Failed to send email via SMTP.", "error": err}), 500

    print(f"DEBUG: Validation request created with ID: {request_id}")
    return jsonify({"message": "Validation email sent successfully.", "request_id": request_id}), 200


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
        title, action_text, color = "Confirm AI Report", "Confirm", "#4CAF50"
    elif action == 'decline':
        title, action_text, color = "Decline AI Report", "Decline", "#F44336"
    else:
        return "Error: Invalid action specified.", 400

    html_content = f"""
    <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>{title}</title>
        <style>body {{ font-family: 'Inter', sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }} .container {{ max-width: 800px; margin: 0 auto; background-color: #ffffff; padding: 25px; border-radius: 12px; box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15); }} h1 {{ color: {color}; text-align: center; border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 25px; }} .report-box {{ background-color: #f0f8ff; border: 1px solid #cceeff; padding: 15px; border-radius: 8px; margin-bottom: 20px; white-space: pre-wrap; font-family: monospace; font-size: 0.95em; }} label {{ display: block; margin-bottom: 10px; font-weight: bold; color: #333; }} textarea {{ width: 100%; padding: 12px; box-sizing: border-box; border: 2px solid #ccc; border-radius: 6px; resize: vertical; min-height: 150px; transition: border-color 0.3s; }} textarea:focus {{ border-color: #007acc; outline: none; }} .submit-btn {{ background-color: {color}; color: white; padding: 14px 25px; border: none; border-radius: 6px; cursor: pointer; font-size: 17px; font-weight: bold; width: 100%; margin-top: 20px; transition: background-color 0.3s, transform 0.1s; }} .submit-btn:hover {{ opacity: 0.9; transform: translateY(-1px); }} @media (max-width: 600px) {{ .container {{ padding: 15px; }} h1 {{ font-size: 1.5em; }} }}</style>
    </head>
    <body><div class="container"><h1>{title}</h1><h2>AI Report Content</h2><div class="report-box">{report_content}</div>
            <form action="/api/handle-validation" method="post"><input type="hidden" name="request_id" value="{request_id}"><input type="hidden" name="action" value="{action}">
                <label for="comments">Validator Comments (Required for final decision):</label><textarea id="comments" name="comments" placeholder="Enter comments regarding your decision..." required></textarea>
                <button type="submit" class="submit-btn">{action_text} and Submit to Database</button>
            </form><p style="text-align: center; margin-top: 30px;"><small>Report ID: {request_id}</small></p></div></body></html>"""
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

    # 2. Prepare Final RFQ Payload
    original_rfq_payload = request_data.get('rfq_payload')

    if not isinstance(original_rfq_payload, dict):
        db_submission_error = "FATAL: Original RFQ structured data ('rfq_payload') is missing or invalid."
        db_submission_status = "FAILED"
        payload_to_send = None
    else:
        payload_to_send = {
            **original_rfq_payload,
            "status": action.upper(),       
            "validator_comments": comments   
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
        project_id = request_data.get('project_id')
        report_content = request_data.get('report_content')
        
        if project_id and report_content:
            monday_ok, monday_err = update_monday_rfq_report(project_id, report_content)
            if monday_ok:
                monday_update_status = "SUCCESS"
            else:
                monday_update_status = "FAILED"
                monday_update_error = monday_err
        else:
            monday_update_status = "FAILED"
            monday_update_error = "Missing project_id or report_content in stored data."

    # 5. Send Final Email to User
    user_email = request_data['user_email']
    color = '#4CAF50' if action == 'confirm' else '#F44336'
    subject = f"✅ Report {action.upper()} - Final Decision" if action == 'confirm' else f"❌ Report {action.upper()} - Final Decision"

    final_email_body_html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height: 1.6;"><h2>Your AI Report Validation Result</h2><p>The validator, <strong>{request_data['validator_email']}</strong>, has finished reviewing your report.</p><hr>
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

@app.route('/apollo/search', methods=['POST'])
@app.route('/api/v1/mixed_people/search', methods=['POST'])
def search_people_simple():
    """
    Search for people across multiple organizations.
    Loops through each organization and aggregates results.
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
            all_contacts.extend(contacts)

            pagination = response_data.get("pagination", {})
            total_entries += pagination.get("total_entries", 0)
            organizations_searched.append(org_name)

            print(f"✓ Found {len(contacts)} contacts from {org_name}")

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
        return jsonify(response_data), 200
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
    
    # Validate request
    is_valid, error_msg, req = validate_bulk_enrich_request(data)
    if not is_valid:
        return jsonify({"status": "error", "message": error_msg}), 400

    if req["reveal_phone_number"] and not req["webhook_url"]:
        return jsonify({
            "status": "error",
            "message": "webhook_url is mandatory when reveal_phone_number is True"
        }), 400

    payload: Dict[str, Any] = {"details": req["details"]}
    if req["reveal_personal_emails"]:
        payload["reveal_personal_emails"] = True
    if req["reveal_phone_number"]:
        payload["reveal_phone_number"] = True
        payload["webhook_url"] = req["webhook_url"]

    try:
        client = get_apollo_client(x_api_key)
        response_data = client.bulk_enrich(payload)
        return jsonify(response_data), 200
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", 500)
        text = getattr(e.response, "text", str(e))
        return jsonify({"status": "error", "message": text}), status
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
            'validator_comments', 'status'
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
            final_validator_comments, final_status
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


#===========================================Sales feedback =================
def get_conn2():
    return psycopg2.connect(
        host="avo-adb-002.postgres.database.azure.com",
        user="administrationSTS",
        password="St$@0987",
        dbname="Sales_feedback"
    )
 
# --- Schéma attendu & validation basique ---
STEP_KEYS = [
    "table_structure_and_layout",
    "usability_and_data_handling",
    "speed_and_performance",
    "data_relevance_and_sts_support",
    "suggestions_and_needs",
    "overall_satisfaction_and_impact"
]
 
def validate_payload(payload: dict):
    # Champs de tête
    if "sales_person_text" not in payload or not isinstance(payload["sales_person_text"], str):
        return False, "Missing or invalid 'sales_person_text'"
    # date facultative (ISO YYYY-MM-DD si fournie)
    if "date" in payload and payload["date"]:
        try:
            datetime.strptime(payload["date"], "%Y-%m-%d")
        except ValueError:
            return False, "Invalid 'date' format. Use YYYY-MM-DD."
    # 6 sections présentes et de type dict (on stockera en JSON)
    for k in STEP_KEYS:
        if k not in payload:
            return False, f"Missing section '{k}'"
        if not isinstance(payload[k], dict):
            return False, f"Section '{k}' must be an object (dict)"
    return True, None
 
@app.route("/api/feedback", methods=["POST"])
def insert_feedback():
    try:
        data = request.get_json(force=True, silent=False)
 
        ok, err = validate_payload(data)
        if not ok:
            return jsonify({"error": err}), 400
 
        # Sérialiser les 6 sections en JSON
        serialized = {k: json.dumps(data.get(k, {}), ensure_ascii=False) for k in STEP_KEYS}
 
        sales_person_text = data.get("sales_person_text")
        date_str = data.get("date") or datetime.utcnow().strftime("%Y-%m-%d")
 
        sql = """
            INSERT INTO feedback_survey (
                sales_person_text, date,
                table_structure_and_layout,
                usability_and_data_handling,
                speed_and_performance,
                data_relevance_and_sts_support,
                suggestions_and_needs,
                overall_satisfaction_and_impact
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """
 
        vals = (
            sales_person_text, date_str,
            serialized["table_structure_and_layout"],
            serialized["usability_and_data_handling"],
            serialized["speed_and_performance"],
            serialized["data_relevance_and_sts_support"],
            serialized["suggestions_and_needs"],
            serialized["overall_satisfaction_and_impact"]
        )
 
        with get_conn2() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, vals)
                new_id = cur.fetchone()["id"]
 
        return jsonify({"message": "Feedback saved", "id": new_id}), 201
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
