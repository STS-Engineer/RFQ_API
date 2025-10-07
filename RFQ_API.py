import os
import uuid
import datetime
import sys
import json 
import requests
import psycopg2
from psycopg2 import OperationalError, errorcodes, extras
from flask import Flask, request, jsonify
from flask_mail import Mail, Message


# --- APPLICATION CONFIGURATION (HARDCODED FOR DEPLOYMENT/TESTING) ---

# PostgreSQL Configuration
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}

# Flask Application Initialization
app = Flask(__name__)

# URL Configuration
BASE_URL = "https://rfq-api.azurewebsites.net" 
RFQ_SUBMISSION_API_URL = "https://rfq-api.azurewebsites.net/api/rfq/submit" 

# Flask-Mail Configuration for Outlook SMTP (UNAUTHENTICATED RELAY)
app.config['MAIL_SERVER'] = 'avocarbon-com.mail.protection.outlook.com'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False 
app.config['MAIL_DEFAULT_SENDER'] = 'administration.STS@avocarbon.com'

mail = Mail(app)


# --- DATABASE CONNECTION UTILITY ---

def get_db():
    """Returns a PostgreSQL database connection and a RealDictCursor."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        return conn, cursor
    except OperationalError as e:
        print(f"PostgreSQL connection failed: {e}")
        if conn: conn.close()
        error_message = f"Database connection failed: {str(e)}"
        raise ConnectionError(error_message)
    except Exception as e:
        if conn: conn.close()
        raise e


def convert_to_boolean(value):
    """Safely converts string/bool input to a Python boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['yes', 'true']
    return False 


# --- PERSISTENT STATE MANAGEMENT FUNCTIONS (PostgreSQL) ---

def get_request_data(request_id):
    """
    Retrieves request data regardless of status. 
    NOTE: Removed 'AND status = PENDING' filter to allow checking final status.
    """
    conn = None
    try:
        conn, cursor = get_db()
        # Retrieve the data regardless of status
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

# --- Helper Function for Synchronous Mail Sending (Non-Authenticated) ---
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


# ----------------------------------------------------------------------
# --- 1. SEND REPORT ENDPOINT (Receives AI data and emails validator) ---
# ----------------------------------------------------------------------
@app.route('/api/send-report', methods=['POST', 'PUT'])
def send_report_for_validation():
    print(f"DEBUG: Endpoint /api/send-report hit with method: {request.method}")
    data = request.json

    # 1. Get Data from Assistant
    report_content = data.get('report')
    user_email = data.get('user_email')
    validator_email = data.get('validator_email')
    rfq_payload = data.get('rfq_payload')

    if not all([report_content, user_email, validator_email, rfq_payload]):
        print("DEBUG: Missing required data.")
        return jsonify({"message": "Missing required data (report, user_email, validator_email, rfq_payload)"}), 400

    # 2. Store State and Generate Token (Now uses PostgreSQL)
    request_id = str(uuid.uuid4())
    request_data = {
        'report_content': report_content,
        'user_email': user_email,
        'validator_email': validator_email,
        'rfq_payload': rfq_payload, 
        'status': 'PENDING',
        'validator_comments': None,
        'created_at': datetime.datetime.now().isoformat()
    }
    set_request_data(request_id, request_data) # Store in DB

    # 3. Create Interactive Links
    confirm_link = f"{BASE_URL}/validate-page?id={request_id}&action=confirm"
    decline_link = f"{BASE_URL}/validate-page?id={request_id}&action=decline"

    # 4. Construct Email (HTML is essential for buttons)
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


# ---------------------------------------------------------------------------
# --- 2. LANDING PAGE ROUTE (Validator views report and enters comments) ---
# ---------------------------------------------------------------------------
@app.route('/validate-page')
def validate_page():
    print(f"DEBUG: Endpoint /validate-page hit. Args: {request.args}")
    request_id = request.args.get('id')
    action = request.args.get('action') 

    # Retrieval from PostgreSQL
    request_data = get_request_data(request_id)
    
    if request_data is None:
        return "Error: Invalid or expired validation request.", 404

    # --- FIX: Extract necessary variables now, before conditional checks ---
    report_content = request_data['report_content']
    status = request_data['status']
    
    # Extract other variables needed for the 'Processed' status page below
    validated_at = request_data.get('validated_at', 'N/A')
    comments = request_data.get('validator_comments', 'No comments provided.')

    if status != 'PENDING':
        # Case 3: Already Processed (CONFIRMED or DECLINED) - Show Status Page
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

    # Case 2: Status is PENDING - Proceed to show the submission form

    if action == 'confirm':
        title, action_text, color = "Confirm AI Report", "Confirm", "#4CAF50"
    elif action == 'decline':
        title, action_text, color = "Decline AI Report", "Decline", "#F44336"
    else:
        return "Error: Invalid action specified.", 400

    # The HTML for the responsive landing page (uses defined report_content)
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

# --------------------------------------------------------------------------------------------------
# --- 3. VALIDATION HANDLER ENDPOINT (Receives form data, updates state, and submits to DB) ---
# --------------------------------------------------------------------------------------------------
@app.route('/api/handle-validation', methods=['POST'])
def handle_validation():
    print(f"DEBUG: Endpoint /api/handle-validation hit with method: {request.method}")
    
    request_id = request.form.get('request_id')
    action = request.form.get('action')
    comments = request.form.get('comments')

    # Retrieval from PostgreSQL
    request_data = get_request_data(request_id)

    if request_data is None:
        return "Error: Invalid or expired validation request.", 404
    # The PENDING status check is redundant here because the validation form should only be accessible
    # if the status is PENDING. However, we keep it for defense in depth.
    if request_data['status'] != 'PENDING':
        return "Error: This report has already been processed.", 400

    # 1. Update Persistent State (Status and Comments)
    request_data['status'] = action.upper()
    request_data['validator_comments'] = comments
    request_data['validated_at'] = datetime.datetime.now().isoformat()
    set_request_data(request_id, request_data) # Update state in DB

    # --- 2. Prepare Final RFQ Payload for External API ---
    original_rfq_payload = request_data.get('rfq_payload')
    
    if not isinstance(original_rfq_payload, dict):
        db_submission_error = "FATAL: Original RFQ structured data ('rfq_payload') is missing or invalid."
        db_submission_status = "FAILED"
        payload_to_send = None
    else:
        # Create the final payload by safely merging the original data with the new validation fields
        payload_to_send = {
            **original_rfq_payload,
            "status": [action.upper()],        # CRITICAL FIX: Array for PostgreSQL
            "validator_comments": comments   
        }
        db_submission_status = "PENDING"

    # 3. Synchronously Submit to External Database API
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

    # 4. Prepare and Send Final Email to the User
    user_email = request_data['user_email']
    color = '#4CAF50' if action == 'confirm' else '#F44336'
    subject = f"✅ Report {action.upper()} - Final Decision" if action == 'confirm' else f"❌ Report {action.upper()} - Final Decision"
    
    final_email_body_html = f"""
    <html><body style="font-family: Arial, sans-serif; line-height: 1.6;"><h2>Your AI Report Validation Result</h2><p>The validator, <strong>{request_data['validator_email']}</strong>, has finished reviewing your report.</p><hr>
            <h3 style="color: {color};">Decision: {action.upper()}</h3><h4>Validator Comments:</h4><div style="border-left: 5px solid {color}; padding: 10px; background-color: #e9f5ff; border-radius: 0 4px 4px 0; margin-bottom: 20px;"><p style="margin: 0;">{comments}</p></div>
            <h4>Database Submission Status: <span style="color: {'#4CAF50' if db_submission_status == 'SUCCESS' else '#F44336'};">{db_submission_status}</span></h4>
            {f'<p style="color: #F44336;">Error Details: {db_submission_error}</p>' if db_submission_status != 'SUCCESS' else ''}<p style="margin-top: 20px;">Thank you for using the AI Assistant.</p></body></html>
    """
    msg = Message(subject, recipients=[user_email])
    msg.html = final_email_body_html
    safe_send_mail(msg) 

    # 5. Show Success/Failure Page to Validator
    if db_submission_status != 'SUCCESS':
        return f"""<!DOCTYPE html><html lang="en"><head><title>Submission Failed</title><style>body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #fef2f2; }} .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-left: 6px solid #dc2626; }} h1 {{ color: #dc2626; margin-bottom: 6px; }} pre {{ text-align: left; white-space: pre-wrap; background:#fff7ed; padding:12px; border-radius:6px; border:1px solid #fed7aa }}</style></head>
        <body><div class="card"><h1>Decision Saved, Database Submission Failed</h1><p>Your validation decision was saved, but the final RFQ data could not be submitted to the external database API.</p><h3 style="margin-top:18px;">Error Details</h3><pre>{db_submission_error}</pre><p>The user was notified of this failure via email.</p></div></body></html>""", 500

    return f"""<!DOCTYPE html><html lang="en"><head><title>Success</title><style>body {{ font-family: 'Inter', sans-serif; text-align: center; padding: 50px; background-color: #f4f4f4; }} .card {{ max-width: 720px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); border-left: 6px solid {color}; }} h1 {{ color: {color}; }} p {{ color: #4b5563; }} .pill {{ display:inline-block; padding: 6px 10px; border-radius: 9999px; background:#ecfdf5; color:#065f46; font-weight:600; }}</style></head>
    <body><div class="card"><h1>Validation Complete!</h1><p class="pill">Decision: {action.upper()}</p><p style="margin-top:14px;">Your decision has been recorded and the final RFQ data was successfully submitted to the database.</p><p style="margin-top:18px;"><a href="/" style="color:#2563eb; text-decoration:none;">Return to start</a></p></div></body></html>"""


# --- 3. API ENDPOINTS (Submission and Retrieval Endpoints) ---

@app.route('/api/rfq/submit', methods=['POST'])
def submit_rfq_data():
    """
    Receives structured RFQ data, including top-level validation status and comments, 
    and inserts it into the main and contact tables. (External API logic)
    """
    data = request.get_json()
    conn = None
    rfq_id = None
    
    if not data:
        return jsonify({"status": "error", "message": "No data provided"}), 400

    rfq_id = data.get('rfq_id')
    if not rfq_id:
        return jsonify({"status": "error", "message": "Missing required field: rfq_id"}), 400

    # --- 1. Extract and Normalize Validation Data ---
    # Status is now a simple string, validator_comments is text.
    final_status = data.pop('status', None) 
    final_validator_comments = data.pop('validator_comments', None)

    # Normalize status from list to string if it somehow arrives as an array
    if isinstance(final_status, list) and final_status:
        final_status = final_status[0]
    
    # Helper to handle missing/empty string numerical inputs gracefully
    def get_numeric(key):
        val = data.get(key)
        # Convert empty strings to None, otherwise psycopg2 may crash attempting to cast "" to numeric
        return val if val != '' else None
    
    try:
        conn, cursor = get_db()
        
        # --- CONTACT HANDLING: Find or Create Contact to get FK ---
        contact_data = data.get('contact', {})
        contact_email = contact_data.get('email')
        
        if not contact_email:
            conn.close()
            return jsonify({"status": "error", "message": "Missing required contact field: email"}), 400

        contact_id_fk = None
        cursor.execute("SELECT contact_id FROM contact WHERE contact_email = %s", (contact_email,))
        existing_contact = cursor.fetchone()

        if existing_contact:
            contact_id_fk = existing_contact['contact_id']
        else:
            insert_contact_sql = "INSERT INTO contact (contact_role, contact_email, contact_phone) VALUES (%s, %s, %s) RETURNING contact_id;"
            cursor.execute(insert_contact_sql, (contact_data.get('role'), contact_data.get('email'), contact_data.get('phone')))
            contact_id_fk = cursor.fetchone()['contact_id']

        # NOTE: Skip the 'contact' object in the payload as we have the FK.
        data.pop('contact', None) 
        
        # --- 2. MAIN RFQ INSERTION (FIXED ALIGNMENT) ---

        # Explicitly list all 36 columns (34 original + contact_id_fk + validator_comments + status)
        COLUMN_NAMES = [
            'rfq_id', 'customer_name', 'application', 'product_line', 'customer_pn', 'revision_level',
            'delivery_zone', 'delivery_plant', 'sop_year', 'annual_volume', 'rfq_reception_date',
            'quotation_expected_date', 'target_price_eur', 'delivery_conditions', 'payment_terms',
            'business_trigger', 'entry_barriers', 'product_feasibility_note', 'manufacturing_location',
            'risks', 'decision', 'design_responsibility', 'validation_responsibility', 'design_ownership',
            'development_costs', 'technical_capacity', 'scope_alignment', 'overall_feasibility',
            'customer_status', 'strategic_note', 'final_recommendation', 'contact_id_fk', 
            'validator_comments', 'status' # New Validation Columns
        ]
        
        # Build the list of values (34 + FK + 2 Validation)
        main_values = [
            # 1-6. VARCHAR fields
            data.get('rfq_id'), data.get('customer_name'), data.get('application'), data.get('product_line'), data.get('customer_pn'), data.get('revision_level'),
            # 7-8. VARCHAR fields
            data.get('delivery_zone'), data.get('delivery_plant'), 
            # 9-10. INTEGER fields (Use get_numeric)
            get_numeric('sop_year'), get_numeric('annual_volume'), 
            # 11-12. DATE fields
            data.get('rfq_reception_date'), data.get('quotation_expected_date'), 
            # 13. NUMERIC field (Use get_numeric)
            get_numeric('target_price_eur'), 
            # 14-25. VARCHAR/TEXT fields
            data.get('delivery_conditions'), data.get('payment_terms'), data.get('business_trigger'), data.get('entry_barriers'), data.get('product_feasibility_note'), 
            data.get('manufacturing_location'), data.get('risks'), data.get('decision'), data.get('design_responsibility'), data.get('validation_responsibility'), 
            data.get('design_ownership'), data.get('development_costs'), 
            # 26-27. BOOLEAN fields (Use convert_to_boolean)
            convert_to_boolean(data.get('technical_capacity')), convert_to_boolean(data.get('scope_alignment')), 
            # 28-31. VARCHAR/TEXT fields
            data.get('overall_feasibility'), data.get('customer_status'), data.get('strategic_note'), data.get('final_recommendation'), 
            # 32. FOREIGN KEY (INTEGER)
            contact_id_fk, 
            # 33-34. VALIDATOR FIELDS (TEXT/VARCHAR)
            final_validator_comments, final_status 
        ]
        
        columns_sql = ', '.join(COLUMN_NAMES)
        placeholders_sql = ', '.join(['%s'] * len(COLUMN_NAMES))

        insert_main_sql = f"INSERT INTO main ({columns_sql}) VALUES ({placeholders_sql})"
        
        cursor.execute(insert_main_sql, main_values)
        conn.commit()

        return jsonify({"status": "success", "message": "RFQ data successfully stored.", "rfq_id": rfq_id}), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except OperationalError as e:
        if conn: conn.rollback()
        pg_error_message = getattr(e, 'pgerror', None)
        detail_message = pg_error_message.strip() if pg_error_message else str(e)

        return jsonify({"status": "error", "message": f"Database insertion failed (PostgreSQL Error): {detail_message}", "rfq_id": rfq_id if rfq_id else None}), 500
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


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
        SELECT m.*, c.contact_role, c.contact_email, c.contact_phone
        FROM main m
        INNER JOIN contact c ON m.contact_id_fk = c.contact_id
        WHERE {where_sql}
        ORDER BY m.rfq_reception_date DESC;
        """
        
        cursor.execute(select_sql, tuple(query_params))
        results = cursor.fetchall()
        
        if not results:
            return jsonify({"status": "success", "message": "No RFQ records found matching the criteria.", "data": []}), 200

        # --- Formatting Results ---
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

        return jsonify({"status": "success", "message": f"Retrieved {len(formatted_results)} RFQ record(s).", "data": formatted_results}), 200

    except ConnectionError as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    except OperationalError as e:
        print(f"PostgreSQL query failed: {e}")
        return jsonify({"status": "error", "message": f"Database query failed: {str(e)}", "query_params": request.args}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
        
    finally:
        if conn: conn.close()


@app.route('/api/products', methods=['GET'])
def retrieve_products_modified():
    """Retrieves product data from the 'products' table."""
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
        
        return jsonify({"query": product_name if product_name else "All Products", "products": products_data, "source": "database"}), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500
    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({"error": "Error retrieving products from the database", "details": pg_error_message}), 400
        
    finally:
        if conn: conn.close()


@app.route('/api/product-lines', methods=['GET'])
def retrieve_product_line_modified():
    """Retrieves product line data from the 'product_lines' table using a required ID."""
    product_line_id = request.args.get('productLineId') 

    if not product_line_id:
        return jsonify({"error": "Missing required query parameter: productLineId", "details": "The ID of the product line must be provided to retrieve its details."}), 400 

    conn = None
    try:
        conn, cursor = get_db()
        query = "SELECT * FROM public.product_lines WHERE id = %s;"
        cursor.execute(query, (product_line_id,))

        product_line_data = cursor.fetchall()

        return jsonify({"query": product_line_id, "productLine": product_line_data, "source": "database"}), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500
    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({"error": "Error retrieving product-line items from the database", "details": pg_error_message}), 400

    finally:
        if conn: conn.close()


@app.route('/api/product-lines/list', methods=['GET'])
def list_product_lines():
    """Retrieves the list of all product lines (ID and Name) for selection menus."""
    conn = None
    try:
        conn, cursor = get_db()
        query = "SELECT id, name AS product_line_name, type_of_products AS description_snippet FROM public.product_lines ORDER BY id;"
        cursor.execute(query)

        product_lines_list = cursor.fetchall()

        return jsonify({"productLinesList": product_lines_list, "count": len(product_lines_list), "source": "database"}), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500
    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({"error": "Error retrieving product lines list from the database", "details": pg_error_message}), 400

    finally:
        if conn: conn.close()

@app.route('/api/product-lines/details', methods=['GET'])
def get_product_line_by_product_name():
    """Retrieves product line details based on a given product name."""
    product_name = request.args.get('productName')

    if not product_name:
        return jsonify({"error": "Missing required query parameter: productName", "details": "Provide a valid product name to retrieve product-line details."}), 400

    conn = None
    try:
        conn, cursor = get_db()

        query = """
            SELECT pl.id AS product_line_id, pl.name AS product_line_name, pl.type_of_products,
                   pl.manufacturing_locations, pl.design_center, pl.product_line_manager,
                   pl.type_of_customers, pl.metiers, pl.strength, pl.weakness, pl.perspectives,
                   pl.history, p.id AS product_id, p.product_name, p.description AS product_description,
                   p.product_definition, p.operating_environment, p.technical_parameters
            FROM public.products p
            INNER JOIN public.product_lines pl ON p.product_line_id = pl.id
            WHERE p.product_name ILIKE %s;
        """

        cursor.execute(query, (f"%{product_name}%",))
        results = cursor.fetchall()

        if not results:
            return jsonify({"status": "success", "message": "No product line found matching the provided product name.", "data": []}), 200

        return jsonify({"status": "success", "message": f"Retrieved {len(results)} product-line record(s).", "data": results}), 200

    except ConnectionError as e:
        return jsonify({"error": str(e), "details": "Database connection failed."}), 500
    except OperationalError as e:
        pg_error_message = getattr(e, 'pgerror', str(e))
        return jsonify({"error": "Error retrieving product-line details from the database", "details": pg_error_message}), 500
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(error_details)
        return jsonify({"error": "An unexpected error occurred", "details": str(e)}), 500

    finally:
        if conn: conn.close()


if __name__ == '__main__':
    app.run(debug=True)
