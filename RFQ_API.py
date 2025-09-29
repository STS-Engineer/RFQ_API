import os
import psycopg2
from psycopg2 import OperationalError, errorcodes, extras
from flask import Flask, request, jsonify

# --- 1. CONFIGURATION ---
# IMPORTANT: Replace these placeholders with your actual PostgreSQL credentials.
DB_CONFIG = {
    "host": "avo-adb-002.postgres.database.azure.com",
    "database": "RFQ_DATA",
    "user": "administrationSTS",
    "password": "St$@0987"
}

app = Flask(__name__)

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

# --- 3. API ENDPOINTS ---

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

            
if __name__ == '__main__':
    # When running locally, set the FLASK_APP environment variable.
    # On Azure, gunicorn will handle execution.
    app.run(debug=True)
