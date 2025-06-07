import os
import sqlite3
import psycopg2
import requests
from pathlib import Path

# URL for the default chess puzzle database
DB_URL = 'https://github.com/JackScallan02/chess-puzzle-kit/releases/download/v0.1.0/lichess_db_puzzle.db'
DEFAULT_PATH = Path.home() / '.chess_puzzles' / 'lichess_db_puzzle.db'

_connections = {}  # Cache database connections
_current_db_path = None  # Global override for DB path/URI


def get_database_type(db_connection):
    """
    Determines the type of database from a given connection object.

    Args:
        db_connection: An active database connection object.

    Returns:
        str: "sqlite3", "postgresql", or "unknown".
    """
    if db_connection is None:
        return "unknown"
    module_name = db_connection.__class__.__module__
    if "sqlite3" in module_name:
        return "sqlite3"
    elif "psycopg2" in module_name or "psycopg" in module_name:
        return "postgresql"
    else:
        return "unknown"


def set_db_path(db_path_or_uri):
    """
    Sets the global database path or URI for subsequent connections.

    Args:
        db_path_or_uri (str or Path): The path to a SQLite file or a PostgreSQL URI.
    """
    global _current_db_path
    _current_db_path = db_path_or_uri


def download_default_db():
    """
    Downloads the default SQLite chess puzzle database to DEFAULT_PATH.
    """
    print(f"Default database not found. Downloading to {DEFAULT_PATH}...")
    os.makedirs(DEFAULT_PATH.parent, exist_ok=True)
    try:
        with requests.get(DB_URL, stream=True) as r:
            r.raise_for_status()
            with open(DEFAULT_PATH, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
        if not DEFAULT_PATH.exists():
             raise FileNotFoundError(f"Database file not found after download attempt at {DEFAULT_PATH}")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to download default database: {e}")

def initialize_connection(db_path_or_uri=None):
    """
    Initializes the database connection.

    If no path or URI is provided, this function will check for the default
    SQLite database, download it if it does not exist, and set it as the
    active database for subsequent operations.

    Args:
        db_path_or_uri (str or Path, optional): The path to a SQLite file or a
            PostgreSQL URI. If None, uses the default database path.
            Defaults to None.

    Returns:
        An active database connection object.
    """
    if db_path_or_uri is None:
        # If using the default path, check for existence and download if needed.
        path_to_check = DEFAULT_PATH
        if not path_to_check.exists():
            download_default_db()
        set_db_path(path_to_check)
    else:
        # If a specific path is provided, just set it.
        set_db_path(db_path_or_uri)
    
    return get_connection()


def get_connection():
    """
    Retrieves or establishes a database connection based on prior initialization.
    Caches connections for performance.

    Raises:
        ConnectionError: If the connection has not been initialized via
                         `initialize_connection()` or `set_db_path()`.

    Returns:
        An active database connection object.
    """
    if not _current_db_path:
        raise ConnectionError(
            "Database connection not initialized. "
            "Please call `initialize_connection()` before trying to connect."
        )

    path_or_uri = _current_db_path
    conn_key = str(path_or_uri)
    
    if conn_key in _connections and _connections[conn_key]:
        try:
            # Ping PostgreSQL connection to check if it's alive
            if get_database_type(_connections[conn_key]) == "postgresql":
                _connections[conn_key].cursor().execute("SELECT 1")
            # SQLite connections are file-based and don't typically "die"
            return _connections[conn_key]
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            del _connections[conn_key]  # Stale connection, remove from cache

    # Handle PostgreSQL connection
    if isinstance(path_or_uri, str) and path_or_uri.startswith(("postgresql://", "postgres://")):
        try:
            conn = psycopg2.connect(path_or_uri)
            _connections[conn_key] = conn
            return conn
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    # Handle SQLite connection
    path = Path(path_or_uri)
    if not path.is_absolute():
       path = Path.cwd() / path
    
    if not path.exists():
        raise FileNotFoundError(f"Database file not found at '{path}'. Please ensure the path is correct.")

    try:
        conn = sqlite3.connect(path)
        _connections[conn_key] = conn
        return conn
    except sqlite3.Error as e:
        raise ConnectionError(f"Failed to connect to SQLite database at '{path}': {e}")


def close_all_connections():
    """
    Closes all currently cached database connections.
    """
    global _connections
    for path, conn in list(_connections.items()):
        try:
            conn.close()
        except Exception as e:
            print(f"Error closing connection to {path}: {e}")
    _connections = {}