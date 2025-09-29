import json
from flask import Flask, request, jsonify
# Import psycopg2 for PostgreSQL connection
import psycopg2 
from psycopg2 import extras
from psycopg2 import Error as Psycopg2Error

# --- Configuration ---
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# TODO: REPLACE WITH YOUR ACTUAL POSTGRESQL CREDENTIALS
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}
# --- Database Connection and Utility Functions ---

def get_db():
    """Returns a PostgreSQL database connection."""
    try:
        # Establish connection using the configuration dictionary
        conn = psycopg2.connect(**DB_CONFIG)
        # Set the row factory to return dictionaries (useful for jsonify)
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        return conn, cursor
    except Psycopg2Error as e:
        print(f"PostgreSQL connection failed: {e}")
        # Raise the error to be handled by the API endpoint
        raise e


# NOTE: init_db() function and its call are REMOVED 
# as the user confirmed the database structure is already created.

# --- API Endpoint: Submit Data ---

@app.route('/api/rfq/submit', methods=['POST'])
def submit_rfq_data():
    """
    Receives RFQ data via POST request and inserts it into
    the 'main' and 'contact' database tables using PostgreSQL.
    """
    conn = None
    cursor = None
    rfq_id = None # Initialize rfq_id outside try block for better scope visibility
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided"}), 400
            
        # 1. Use RFQ ID provided in the payload (as per new requirement)
        rfq_id = data.get('rfq_id')
        if not rfq_id:
            return jsonify({"status": "error", "message": "Missing required field: rfq_id"}), 400

        # 2. Establish connection
        conn, cursor = get_db()
        
        # 3. Extract and prepare main data fields
        # Note: We use 1/0 for boolean in the data preparation, 
        # but Psycopg2 handles Python's True/False correctly for PostgreSQL BOOLEAN.
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
            # Convert string representations to Python Booleans for PostgreSQL
            'technical_capacity': data.get('technical_capacity', '').lower() == 'yes',
            'scope_alignment': data.get('scope_alignment', '').lower() == 'yes',
            'overall_feasibility': data.get('overall_feasibility'),
            'customer_status': data.get('customer_status'),
            'strategic_note': data.get('strategic_note'),
            'final_recommendation': data.get('final_recommendation'),
        }

        # 4. Extract and prepare contact data fields
        contact_data = data.get('contact', {})
        contact_fields = {
            'rfq_id_fk': rfq_id,
            'contact_role': contact_data.get('role'),
            'contact_email': contact_data.get('email'),
            'contact_phone': contact_data.get('phone'),
        }

        # Construct and execute INSERT for 'main' table
        main_columns = ', '.join(main_fields.keys())
        # Use Psycopg2 named parameter substitution (%(key)s)
        main_placeholders = ', '.join([f"%({key})s" for key in main_fields.keys()])
        main_sql = f"INSERT INTO main ({main_columns}) VALUES ({main_placeholders})"
        cursor.execute(main_sql, main_fields)

        # Construct and execute INSERT for 'contact' table
        contact_columns = ', '.join(contact_fields.keys())
        contact_placeholders = ', '.join([f"%({key})s" for key in contact_fields.keys()])
        contact_sql = f"INSERT INTO contact ({contact_columns}) VALUES ({contact_placeholders})"
        cursor.execute(contact_sql, contact_fields)

        conn.commit()
        return jsonify({
            "status": "success", 
            "message": "RFQ data successfully stored.", 
            "rfq_id": rfq_id
        }), 201

    except Psycopg2Error as e:
        if conn:
            conn.rollback()
        
        # FIX: Safely access e.pgerror, falling back to str(e) if it's None.
        pg_error = e.pgerror.strip() if e.pgerror else str(e)
        
        return jsonify({
            "status": "error", 
            "message": f"Database insertion failed (PostgreSQL Error): {pg_error}",
            "rfq_id": rfq_id
        }), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --- API Endpoint: Get Data ---

@app.route('/api/rfq/get/<rfq_id>', methods=['GET'])
def get_rfq_data(rfq_id):
    """Retrieves and joins data from both tables for a specific RFQ ID using PostgreSQL."""
    conn = None
    cursor = None
    try:
        conn, cursor = get_db()
        
        query = """
        SELECT 
            m.*, 
            c.contact_role, 
            c.contact_email, 
            c.contact_phone
        FROM 
            main m
        LEFT JOIN 
            contact c ON m.rfq_id = c.rfq_id_fk
        WHERE 
            m.rfq_id = %s
        """
        # Execute query using tuple for positional substitution (%s)
        cursor.execute(query, (rfq_id,))
        row = cursor.fetchone()

        if row is None:
            return jsonify({"status": "error", "message": f"RFQ with ID {rfq_id} not found."}), 404
        
        # Psycopg2 with RealDictCursor returns a dictionary, which is perfect for JSON
        data = dict(row)
        
        # Restructure contact information into a nested object
        contact_info = {
            "role": data.pop('contact_role'),
            "email": data.pop('contact_email'),
            "phone": data.pop('contact_phone'),
        }
        
        # PostgreSQL BOOLEAN types (True/False) are preserved. Convert them to string 'Yes'/'No' for API response clarity
        data['technical_capacity'] = 'Yes' if data['technical_capacity'] else 'No'
        data['scope_alignment'] = 'Yes' if data['scope_alignment'] else 'No'

        data['contact'] = contact_info

        return jsonify({"status": "success", "data": data}), 200

    except Psycopg2Error as e:
        # FIX: Safely access e.pgerror, falling back to str(e) if it's None.
        pg_error = e.pgerror.strip() if e.pgerror else str(e)
        return jsonify({"status": "error", "message": f"Database query failed (PostgreSQL Error): {pg_error}"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"An unexpected error occurred: {e}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == '__main__':
    # Ensure you replace the dummy credentials in DB_CONFIG before running.
    # The application will only start successfully if it can connect to the database.
    app.run(debug=True, port=5000)
