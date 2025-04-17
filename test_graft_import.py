import sqlite3
import sqlite_graft
import csv  # Import the csv module
import os  # Import the os module

# load graft using a temporary (empty) in-memory SQLite database
db = sqlite3.connect(":memory:")
db.enable_load_extension(True)
sqlite_graft.load(db)

# -- Start Volume ID Handling Function --
def get_or_generate_volume_id(filename='volume_id.txt'):
    """Reads volume ID from a file or generates a new one if the file doesn't exist."""
    volume_id = None
    # Check if the volume ID file exists
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            volume_id = f.read().strip()
        if volume_id:
            print(f"Read volume ID from file: {volume_id}")
        else:
            print(f"Volume ID file '{filename}' is empty. Will attempt to generate a new ID.")
            # Ensure we attempt generation if the file was empty
            volume_id = None # Reset volume_id to trigger generation logic

    if not volume_id:
        # If the file doesn't exist or was empty, generate a new volume ID
        print("Generating a new volume ID.")
        try:
            # Use a temporary connection to generate the ID
            conn_gen = sqlite3.connect('file:random?vfs=graft', uri=True)
            cursor_gen = conn_gen.execute('PRAGMA database_list')
            db_list_gen = cursor_gen.fetchall()
            conn_gen.close() # Close the temporary connection

            if db_list_gen and len(db_list_gen) > 0:
                # Assuming the first entry contains the volume ID
                volume_id = db_list_gen[0][2] # The third element is the filename (Volume ID)
                print(f"Generated new volume ID: {volume_id}")
                # Save the new volume ID to the file
                with open(filename, 'w') as f:
                    f.write(volume_id)
                print(f"Saved volume ID to {filename}")
            else:
                print("Error: Could not generate a new volume ID from PRAGMA database_list.")
                return None # Return None on failure
        except sqlite3.Error as e:
            print(f"Error generating volume ID: {e}")
            return None # Return None on failure
        except IOError as e:
             print(f"Error saving volume ID to file '{filename}': {e}")
             # We might have generated an ID but couldn't save it. Return it anyway?
             # For now, let's return None as saving failed, implying inconsistency.
             return None

    return volume_id
# -- End Volume ID Handling Function --

# -- Start Connection Function --
def connect_graft_volume(volume_id):
    """Connects to the specified Graft volume."""
    if not volume_id:
        print("Error: Cannot connect without a valid volume_id.")
        return None
    try:
        print(f"Connecting to volume: {volume_id}")
        conn = sqlite3.connect(f'file:{volume_id}?vfs=graft', uri=True)
        cursor = conn.execute('PRAGMA database_list')
        db_list = cursor.fetchall()
        print("Database list:")
        for db_info in db_list:
            print(db_info)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to volume {volume_id}: {e}")
        return None
# -- End Connection Function --

# -- Start DB Optimization Functions --
def apply_db_optimizations(conn, cache_size_bytes):
    """Applies PRAGMA settings for potentially faster bulk imports."""
    try:
        print("--- Applying import optimizations ---")
        conn.execute("PRAGMA journal_mode = OFF;") # Changed from WAL for potential speed, maybe risky
        conn.execute("PRAGMA synchronous = 0;")
        conn.execute(f"PRAGMA cache_size = -{cache_size_bytes // 1024};") # Negative value uses KiB
        conn.execute("PRAGMA locking_mode = EXCLUSIVE;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        print("Import optimizations applied.")
        return True
    except sqlite3.Error as e:
        print(f"Error applying PRAGMA optimizations: {e}")
        return False

def reset_db_settings(conn):
    """Resets PRAGMA settings to more standard defaults."""
    try:
        print("--- Resetting database settings ---")
        # Reset PRAGMAs to safer defaults
        conn.execute("PRAGMA journal_mode = WAL;") # Reset to WAL
        conn.execute("PRAGMA synchronous = FULL;") # Reset to FULL (safer default)
        conn.execute("PRAGMA locking_mode = NORMAL;")
        # Reset cache_size and temp_store to default by closing usually, but explicitly if needed
        conn.execute("PRAGMA cache_size = -2000;") # Default SQLite cache size (approx 2MB)
        conn.execute("PRAGMA temp_store = DEFAULT;")
        print("Database settings reset.")
        return True
    except sqlite3.Error as e:
        print(f"Error resetting PRAGMA settings: {e}")
        return False
# -- End DB Optimization Functions --


# Function to sanitize column names for SQL
def sanitize_column_name(name):
    # Replace invalid characters with underscore, ensure it doesn't start with a number
    sanitized = ''.join(c if c.isalnum() else '_' for c in name)
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    # Handle potential empty names after sanitization
    return sanitized if sanitized else 'column'


# -- Start CSV Import Function --
def import_csv_data(conn, csv_file_path, table_name):
    """Imports data from a CSV file into a specified SQLite table."""
    cursor = conn.cursor()
    rows_inserted = 0
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader)  # Read the header row
            except StopIteration:
                print(f"Error: CSV file '{csv_file_path}' is empty or has no header.")
                return -3 # Indicate empty file error

            # Sanitize column names
            sanitized_header = [sanitize_column_name(col) for col in header]

            # Ensure unique column names
            final_header = []
            counts = {}
            for col in sanitized_header:
                if col in counts:
                    counts[col] += 1
                    final_header.append(f"{col}_{counts[col]}")
                else:
                    counts[col] = 0
                    final_header.append(col)

            # Dynamically create the CREATE TABLE statement
            # We'll assume all columns are TEXT for simplicity, adjust if needed
            columns_sql = ', '.join([f'"{col}" TEXT' for col in final_header])
            create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});'

            print(f"Creating/Ensuring table with SQL: {create_table_sql}")
            cursor.execute(create_table_sql)

            # Prepare the INSERT statement template
            placeholders = ', '.join(['?'] * len(final_header))
            # Create the list of column names enclosed in double quotes
            column_names_sql = ", ".join([f'"{col}"' for col in final_header])
            insert_sql = f'INSERT INTO "{table_name}" ({column_names_sql}) VALUES ({placeholders});'

            print(f"Preparing to insert data using SQL template: {insert_sql}")

            # Insert data row by row using executemany for potential speedup
            data_to_insert = []
            for row in reader:
                if len(row) == len(final_header):
                    data_to_insert.append(row)
                else:
                    print(f"Skipping row due to column mismatch: {row} (expected {len(final_header)}, got {len(row)})")

            if data_to_insert:
                try:
                    cursor.executemany(insert_sql, data_to_insert)
                    rows_inserted = len(data_to_insert)
                    conn.commit()  # Commit the changes
                    print(f"Successfully imported {rows_inserted} rows into table '{table_name}'.")
                except sqlite3.Error as insert_err:
                    print(f"Error during bulk insert: {insert_err}")
                    conn.rollback() # Rollback on error
                    return -2 # Indicate general error
            else:
                 print("No valid data rows found to insert.")
                 # If header existed but no data, count is 0, which is not an error per se
                 conn.commit() # Commit table creation even if no data

            return rows_inserted

    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
        return -1 # Indicate file not found error
    except Exception as e:
        print(f"An error occurred during import: {e}")
        conn.rollback() # Rollback changes on error
        return -2 # Indicate general error
# -- End CSV Import Function --

# -- Start Verification Function --
def verify_import(conn, table_name, expected_rows=None):
    """Verifies the data import by checking row count and fetching sample data."""
    cursor = conn.cursor()
    try:
        # Check row count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        count_result = cursor.fetchone()
        actual_rows = 0
        if count_result:
            actual_rows = count_result[0]
            print(f"Verification: Found {actual_rows} rows in table '{table_name}'.")
            if expected_rows is not None:
                if actual_rows == expected_rows:
                    print("Row count matches expected count.")
                else:
                    print(f"Warning: Row count mismatch! Expected {expected_rows}, found {actual_rows}.")
        else:
            print(f"Could not retrieve row count for table '{table_name}'.")
            # Don't return False yet, maybe table exists but is empty

        # Fetch and print first 5 rows as sample
        cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 5;')
        sample_rows = cursor.fetchall()
        if sample_rows:
            print("Sample data (first 5 rows):")
            # Fetch column names for better display
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = [info[1] for info in cursor.fetchall()]
            print(f"Columns: {columns}")
            for i, row in enumerate(sample_rows):
                print(f"Row {i+1}: {row}")
        elif actual_rows == 0:
             print("Table is empty, which might be expected.")
        else:
            print("No sample data found, though row count > 0.")

        # Consider verification successful if the table exists and count could be read
        # Modify this logic if stricter verification is needed
        return True

    except sqlite3.Error as e:
        print(f"An error occurred during verification: {e}")
        return False
# -- End Verification Function --


# -- Main Import Process Function --
def run_import_process(csv_file_path, table_name, volume_id_file='volume_id.txt', cache_size_mb=2048, apply_optimizations: bool = False):
    """Orchestrates the CSV import process into a Graft volume."""
    print("--- Starting Import Process ---")
    conn = None # Initialize conn to None

    # 1. Get Volume ID
    volume_id = get_or_generate_volume_id(volume_id_file)
    if not volume_id:
        print("Failed to get or generate volume ID. Aborting.")
        return

    # 2. Connect to Graft Volume
    conn = connect_graft_volume(volume_id)
    if not conn:
        print("Failed to connect to Graft volume. Aborting.")
        return

    # 3. Import and Verify
    try:
        print("--- Checking Graft status before import ---")
        check_graft_status(conn) # Check status before

        cache_size_bytes = cache_size_mb * 1024 * 1024
        # Apply optimizations only if the flag is True
        if apply_optimizations:
            if not apply_db_optimizations(conn, cache_size_bytes):
                print("Warning: Failed to apply all DB optimizations.")
                # Decide whether to continue or abort if optimizations fail
                # For now, continue

        # Import data
        print(f"--- Importing data from '{csv_file_path}' to table '{table_name}' ---")
        inserted_count = import_csv_data(conn, csv_file_path, table_name)

        # Verify import based on return code
        if inserted_count == -1:
            print("Import failed: CSV file not found.")
        elif inserted_count == -2:
            print("Import failed: An error occurred during import.")
        elif inserted_count == -3:
             print("Import failed: CSV file empty or header missing.")
        else: # inserted_count >= 0
            print("--- Starting Verification ---")
            verify_import(conn, table_name, expected_rows=inserted_count if inserted_count > 0 else 0)
            print("--- Verification Complete ---")

        print("--- Checking Graft status after import/verification ---")
        check_graft_status(conn) # Check status after

    except Exception as e:
        print(f"An unexpected error occurred during the import/verification phase: {e}")
    finally:
        if conn:
            # Reset settings only if optimizations were applied
            if apply_optimizations:
                reset_db_settings(conn)
            # Close the connection
            try:
                conn.close()
                print("Database connection closed.")
            except sqlite3.Error as e:
                print(f"Error closing the database connection: {e}")

    print("--- Import Process Finished ---")


def check_graft_status(conn):
    """Checks the status of the Graft volume."""
    result = conn.execute("pragma graft_status")
    print(result.fetchall()[0][0])

# -- Main Execution Logic --
if __name__ == "__main__":
    CSV_FILE = '10k-elen-ring-msg.csv'
    TABLE_NAME = 'messages'
    VOLUME_ID_FILE = 'volume_id.txt'
    CACHE_SIZE_MB = 2048 # Define cache size in MB
    APPLY_OPTIMIZATIONS = False # Set to True to enable DB optimizations

    run_import_process(CSV_FILE, TABLE_NAME, VOLUME_ID_FILE, CACHE_SIZE_MB, apply_optimizations=APPLY_OPTIMIZATIONS)

# 