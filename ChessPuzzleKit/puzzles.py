import pandas as pd
import sqlite3
from ._db import get_connection, set_db_path # Import set_db_path for user convenience

"""
This module manages reading and filtering Lichess puzzles from a SQLite database.
The functions provide methods to filter puzzles by various criteria, retrieve
random puzzles, and write puzzles to a file.

By default, this module uses the 'lichess_db_puzzle.db' database from
https://github.com/JackScallan02/chess-puzzle-kit/releases/tag/v0.1.0,
which is derived from the Lichess puzzle database.

Usage:
>>> from chess_puzzle_kit import puzzles, _db
>>> # Download the default database if you don't have it
>>> # _db.download_default_db()
>>>
>>> # Or, if you have a custom database path:
>>> # _db.set_db_path('/path/to/your/custom_puzzles.db')
>>>
>>> # Get a puzzle
>>> puzzle = puzzles.get_puzzle(themes=['mateIn2'], ratingRange=(1500, 2000))
>>> print(puzzle)

The database contains the following columns:
['PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity', 'NbPlays', 'Themes', 'GameUrl', 'OpeningTags']
"""

class DataSource:
    """
    A class to abstract puzzle data loading from various sources:
    SQLite database, CSV file, or pandas DataFrame.

    Args:
        source (str | pd.DataFrame): Path to SQLite database or CSV file, or a pandas DataFrame.
        from_csv (bool): Whether the file source is a CSV. Ignored if source is a DataFrame.
    """
    def __init__(self, source, from_csv=False):
        self.source = source
        self.from_csv = from_csv
        self._df = None

    def load(self):
        """
        Loads the puzzles into a pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded puzzle data.
        """
        if isinstance(self.source, pd.DataFrame):
            self._df = self.source.copy()
        elif isinstance(self.source, str):
            if self.from_csv:
                self._df = pd.read_csv(self.source)
            else:
                conn = sqlite3.connect(self.source)
                self._df = pd.read_sql_query("SELECT * FROM puzzles", conn)
                conn.close()
        else:
            raise TypeError("Source must be a pandas DataFrame or a file path string.")
        return self._df

    def get(self):
        """
        Returns the loaded DataFrame, or loads it if not already loaded.

        Returns:
            pd.DataFrame: The puzzle data.
        """
        return self._df if self._df is not None else self.load()


def get_puzzle(themes=None, ratingRange=None, popularityRange=None, count=1):
    """
    Retrieves a list of random puzzles based on specified criteria.

    Args:
        themes (list of str, optional): List of themes to filter by. Defaults to None.
        ratingRange (tuple of int, optional): (min_rating, max_rating). Defaults to None.
        popularityRange (tuple of int, optional): (min_popularity, max_popularity). Defaults to None.
        count (int, optional): Number of puzzles to return. Defaults to 1.

    Returns:
        list: A list of puzzle dictionaries matching the criteria.
              Returns an empty list if no puzzles are found.
    """
    conn = get_connection()

    if themes is not None and not isinstance(themes, list):
        raise TypeError("Themes must be a list of strings.")
    if ratingRange is not None and (
        not isinstance(ratingRange, (list, tuple)) or len(ratingRange) != 2
        or not all(isinstance(x, int) for x in ratingRange)
    ):
        raise TypeError("ratingRange must be a list or tuple of two integers.")
    if popularityRange is not None and (
        not isinstance(popularityRange, (list, tuple)) or len(popularityRange) != 2
        or not all(isinstance(x, int) for x in popularityRange)
    ):
        raise TypeError("popularityRange must be a list or tuple of two integers.")
    if not isinstance(count, int) or count <= 0:
        raise ValueError("Count must be a positive integer.")

    query = "SELECT * FROM puzzles WHERE 1=1"
    params = []

    if themes:
        theme_clause = " OR ".join(["themes LIKE ?"] * len(themes))
        query += f" AND ({theme_clause})"
        params.extend([f"%{t}%" for t in themes])
    if ratingRange:
        query += " AND rating BETWEEN ? AND ?"
        params.extend(ratingRange)
    if popularityRange:
        query += " AND popularity BETWEEN ? AND ?"
        params.extend(popularityRange)

    query += " ORDER BY RANDOM() LIMIT ?"
    params.append(count)

    cursor = conn.execute(query, params)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_puzzle_raw(query, params=None):
    """
    Executes a raw SQL query against the puzzle database.

    Warning:
        This function is intended for advanced use. Be cautious about SQL
        injection vulnerabilities if the query string is constructed from
        user input. Use parameterized queries where possible.

    Args:
        query (str): The SQL query to execute.
        params (tuple, optional): The parameters to substitute into the query.

    Returns:
        list: A list of dictionaries representing the query result.
    """
    conn = get_connection()
    cursor = conn.execute(query, params or ())
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_puzzle_by_id(puzzle_id):
    """
    Retrieves a specific puzzle by its PuzzleId.

    Args:
        puzzle_id (str): The unique identifier for the puzzle.

    Returns:
        dict or None: A dictionary representing the puzzle, or None if not found.
    """
    if not isinstance(puzzle_id, str):
        raise TypeError("PuzzleId must be a string.")

    conn = get_connection()
    query = "SELECT * FROM puzzles WHERE PuzzleId = ?"
    cursor = conn.execute(query, (puzzle_id,))
    row = cursor.fetchone()
    if row:
        columns = [col[0] for col in cursor.description]
        return dict(zip(columns, row))
    return None


def get_all_themes():
    """
    Retrieves all unique puzzle themes from the database.

    Returns:
        set: A set of all available theme strings.
    """
    conn = get_connection()
    query = "SELECT DISTINCT Themes FROM puzzles"
    cursor = conn.execute(query)
    themes = set()
    for row in cursor.fetchall():
        if row[0]:
            themes.update(row[0].split(' '))
    return themes


def get_rating_range():
    """
    Gets the minimum and maximum puzzle ratings in the database.

    Returns:
        tuple: (min_rating, max_rating).
    """
    conn = get_connection()
    query = "SELECT MIN(Rating), MAX(Rating) FROM puzzles"
    cursor = conn.execute(query)
    return cursor.fetchone()


def get_popularity_range():
    """
    Gets the minimum and maximum puzzle popularities in the database.

    Returns:
        tuple: (min_popularity, max_popularity).
    """
    conn = get_connection()
    query = "SELECT MIN(Popularity), MAX(Popularity) FROM puzzles"
    cursor = conn.execute(query)
    return cursor.fetchone()


def get_puzzle_attributes():
    """
    Retrieves the column names from the puzzles table.

    Returns:
        set: A set of attribute (column) names.
    """
    conn = get_connection()
    query = "PRAGMA table_info(puzzles)"
    cursor = conn.execute(query)
    # The column name is in the second position (index 1) of the result rows
    return {row[1] for row in cursor.fetchall()}


def write_puzzles_to_file(puzzles, file_path, header=True):
    """
    Writes a list of puzzle dictionaries to a CSV file.

    Args:
        puzzles (list of dict): The list of puzzles to write.
        file_path (str or Path): The path to the output CSV file.
        header (bool, optional): Whether to write the column headers. Defaults to True.
    """
    if not isinstance(puzzles, list):
        raise TypeError("Puzzles must be a list of dictionaries.")
    if not all(isinstance(puzzle, dict) for puzzle in puzzles):
        raise TypeError("Each item in the puzzles list must be a dictionary.")

    df = pd.DataFrame(puzzles)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8', header=header)
    except IOError as e:
        raise IOError(f"Error writing to file {file_path}: {e}")