import os
import sqlite3
import requests
from pathlib import Path

DB_URL = 'https://github.com/JackScallan02/chess-puzzle-kit/releases/download/v0.1.0/lichess_db_puzzle.db'
LOCAL_PATH = Path.home() / '.chess_puzzles' / 'lichess_db_puzzle.db'

_conn = None  # Singleton database connection

def get_connection():
    global _conn
    if _conn is None:
        if not LOCAL_PATH.exists():
            _download_db()
        _conn = sqlite3.connect(LOCAL_PATH)
    return _conn

def _download_db():
    os.makedirs(LOCAL_PATH.parent, exist_ok=True)
    print("Downloading chess puzzle database...")
    with requests.get(DB_URL, stream=True) as r:
        r.raise_for_status()
        with open(LOCAL_PATH, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
