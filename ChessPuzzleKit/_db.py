import os
import sqlite3
import requests
from pathlib import Path

DB_URL = 'https://github.com/JackScallan02/chess-puzzle-kit/releases/download/v0.1.0/lichess_db_puzzle.db'
DEFAULT_PATH = Path.home() / '.chess_puzzles' / 'lichess_db_puzzle.db'

_connections = {}  # Cache connections keyed by path
_current_db_path = None  # Keep track of current db path

def set_db_path(db_path):
    """
    Set a custom database path to be used by get_connection().
    Once set, all puzzle-fetching functions will use this database.

    Args:
        db_path (str or Path): The path to the SQLite database file.
    """
    global _current_db_path
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"Database file not found at {path}")
    _current_db_path = path

def get_connection():
    """
    Returns a connection to the chess puzzle database.

    Uses the custom path set by `set_db_path()` or falls back to the default path.
    The connection is cached to avoid reconnecting on subsequent calls.

    Raises:
        FileNotFoundError: If the database file is not found.

    Returns:
        sqlite3.Connection: A connection to the SQLite database.
    """
    global _current_db_path
    path = _current_db_path or DEFAULT_PATH

    if not path.exists():
        raise FileNotFoundError(
            f"Database file not found at {path}. "
            "You must provide a valid path via `set_db_path()` or call `download_default_db()` first."
        )

    if path not in _connections:
        # Connect to the database, which will be cached.
        _connections[path] = sqlite3.connect(path)

    return _connections[path]

def download_default_db():
    """
    Download the default chess puzzle database to the user's home directory.
    """
    os.makedirs(DEFAULT_PATH.parent, exist_ok=True)
    print("Downloading chess puzzle database...")
    with requests.get(DB_URL, stream=True) as r:
        r.raise_for_status()
        with open(DEFAULT_PATH, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
    print(f"Database downloaded to {DEFAULT_PATH}")

def close_all_connections():
    """
    Closes all cached database connections.
    """
    global _connections
    for path, conn in _connections.items():
        conn.close()
    _connections = {}