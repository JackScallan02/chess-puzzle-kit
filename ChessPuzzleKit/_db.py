import os
import sqlite3
import requests
import csv
from pathlib import Path

# URL for the default chess puzzle database
DB_URL = 'https://github.com/JackScallan02/chess-puzzle-kit/releases/download/v0.1.0/lichess_db_puzzle.db'
# Default path to store the database in the user's home directory
DEFAULT_PATH = Path.home() / '.chess_puzzles' / 'lichess_db_puzzle.db'

_connections = {}  # Cache database connections keyed by their file path
_current_db_path = None  # Stores a custom database path if set by the user

def set_db_path(db_path):
    """
    Set a custom database path to be used by get_connection().
    Once set, all puzzle-fetching functions will use this database.

    Args:
        db_path (str or Path): The path to the SQLite database file.

    Raises:
        FileNotFoundError: If the specified custom database file does not exist.
    """
    global _current_db_path
    path = Path(db_path)
    # If a custom path is set, it must already exist.
    if not path.exists():
        raise FileNotFoundError(f"Custom database file not found at {path}")
    _current_db_path = path

def download_default_db():
    """
    Download the default chess puzzle database to the user's home directory.
    This function creates the necessary directories if they don't exist.
    """
    os.makedirs(DEFAULT_PATH.parent, exist_ok=True)
    print("Downloading chess puzzle database...")
    try:
        with requests.get(DB_URL, stream=True) as r:
            r.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            with open(DEFAULT_PATH, "wb") as f:
                for chunk in r.iter_content(8192): # Iterate over response content in chunks
                    f.write(chunk)
        print(f"Database downloaded to {DEFAULT_PATH}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading database: {e}")
        raise

def get_connection():
    """
    Returns a connection to the chess puzzle database.

    This function first determines the database path (custom or default).
    If the default database is not found, it will attempt to download it automatically.
    If a custom path is set and the file is not found, it raises an error.
    The connection is cached to avoid reconnecting on subsequent calls.

    Raises:
        FileNotFoundError: If a custom database file is not found at the specified path.
        requests.exceptions.RequestException: If an error occurs during database download.

    Returns:
        sqlite3.Connection: A connection to the SQLite database.
    """
    global _current_db_path
    path = _current_db_path or DEFAULT_PATH

    # If the database file does not exist
    if not path.exists():
        # If we are using the default path, attempt to download it
        if path == DEFAULT_PATH:
            print(f"Database not found at {DEFAULT_PATH}. Attempting to download...")
            download_default_db()
            # After download, check if it exists now. If not, something went wrong.
            if not path.exists():
                raise FileNotFoundError(f"Failed to download database to {DEFAULT_PATH}. Please check network connection or permissions.")
        else:
            # If a custom path was specified but the file doesn't exist, raise an error
            raise FileNotFoundError(
                f"Database file not found at {path}. "
                "Ensure the custom path points to an existing database file."
            )

    # If connection to this path is not already cached, create and cache it
    if path not in _connections:
        _connections[path] = sqlite3.connect(path)
        print(f"Connected to database at {path}")
    else:
        print(f"Using cached connection to database at {path}")

    return _connections[path]

def close_all_connections():
    """
    Closes all cached database connections.
    This is useful for ensuring resources are released, e.g., before exiting the application.
    """
    global _connections
    print("Closing all database connections...")
    for path, conn in _connections.items():
        conn.close()
        print(f"Closed connection to {path}")
    _connections = {}

def create_db_from_csv(csv_path, db_path):
    """
    Creates a SQLite database from a CSV file.

    Args:
        csv_path (str or Path): Path to the input CSV file.
        db_path (str or Path): Path where the SQLite database should be created.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        Exception: For any database errors.
    """
    csv_path = Path(csv_path)
    db_path = Path(db_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at {csv_path}")

    # Ensure directory exists for the database file
    os.makedirs(db_path.parent, exist_ok=True)

    print(f"Creating SQLite database at {db_path} from CSV file {csv_path}...")

    # Connect to (or create) the database file
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Read the header row from the CSV to create table columns
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

        # You can customize the table name and schema here:
        table_name = 'puzzles'

        # Drop table if it already exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create table statement with all columns as TEXT (or adjust as needed)
        columns = ', '.join([f'"{col}" TEXT' for col in headers])
        create_table_sql = f"CREATE TABLE {table_name} ({columns})"
        cursor.execute(create_table_sql)

        # Prepare insert statement
        placeholders = ', '.join(['?'] * len(headers))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"

        # Insert all rows
        row_count = 0
        for row in reader:
            cursor.execute(insert_sql, row)
            row_count += 1

    conn.commit()
    conn.close()

    print(f"Database created successfully with {row_count} records.")

    # Optionally, set the created DB as current DB path
    set_db_path(db_path)
