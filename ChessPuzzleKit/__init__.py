# ChessPuzzleKit/__init__.py

"""
ChessPuzzleKit is a Python library for fetching and working with a large database
of chess puzzles from Lichess.
"""

# Import data retrieval functions from the 'puzzles' submodule
from .puzzles import (
    get_all_themes,
    get_puzzle,
    get_puzzle_attributes,
    get_puzzle_by_id,
    get_puzzle_raw,
    get_popularity_range,
    get_rating_range,
    write_puzzles_to_file,
)

# Import essential database management functions from the '_db' submodule
# These are crucial for the user to configure and manage the puzzle database.
from ._db import (
    download_default_db,
    set_db_path,
    close_all_connections,
    get_connection,
    initialize_connection,
    get_database_type,
)

# Define the public API for the package.
# This controls what is imported when a user types 'from ChessPuzzleKit import *'
__all__ = [
    # from _db.py
    "close_all_connections",
    "download_default_db",
    "set_db_path",
    "get_connection",
    "initialize_connection",
    "get_database_type",

    # from puzzles.py
    "get_all_themes",
    "get_popularity_range",
    "get_puzzle",
    "get_puzzle_attributes",
    "get_puzzle_by_id",
    "get_puzzle_raw",
    "get_rating_range",
    "write_puzzles_to_file",
]