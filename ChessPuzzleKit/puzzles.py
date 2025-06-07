import pandas as pd
from ._db import get_connection, get_database_type

"""
This module manages reading and filtering Lichess puzzles from a SQLite or PostgreSQL database.
The functions provide methods to filter puzzles by various criteria, retrieve
random puzzles, and write puzzles to a file.

By default, this module uses the 'lichess_db_puzzle.db' database from
https://github.com/JackScallan02/chess-puzzle-kit/releases/tag/v0.1.0,
which is derived from the Lichess puzzle database.

Usage:
>>> import chess_puzzle_kit as cpk
>>> # Download the default database if you don't have it
>>> # cpk.initialize_connection()
>>>
>>> # Or, if you have a custom database path:
>>> # cpk.initialize_connection('/path/to/your/database/or/connection')
>>>
>>> # Get a puzzle
>>> puzzle = cpk.get_puzzle(themes=['mateIn2'], ratingRange=(1500, 2000))
>>> print(puzzle)
>>>
>>> # Important: Close connections when your application finishes
>>> # cpk.close_all_connections()

The database contains the following columns:
['PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity', 'NbPlays', 'Themes', 'GameUrl', 'OpeningTags']
"""
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
    """
    conn = get_connection()
    db_type = get_database_type(conn)

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

    placeholder = "?" if db_type == "sqlite3" else "%s"
    query = 'SELECT * FROM puzzles WHERE 1=1'
    params = []

    if themes:
        like_operator = "LIKE" if db_type == "sqlite3" else "ILIKE"
        theme_conditions = [f'"Themes" {like_operator} {placeholder}'] * len(themes)
        query += " AND (" + " OR ".join(theme_conditions) + ")"
        params.extend([f"%{t}%" for t in themes])

    if ratingRange:
        query += f' AND CAST("Rating" AS INTEGER) BETWEEN {placeholder} AND {placeholder}'
        params.extend(ratingRange)

    if popularityRange:
        query += f' AND CAST("Popularity" AS INTEGER) BETWEEN {placeholder} AND {placeholder}'
        params.extend(popularityRange)

    random_func = "RANDOM()"
    query += f" ORDER BY {random_func} LIMIT {placeholder}"
    params.append(count)

    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_puzzle_raw(query, params=None):
    """
    Executes a raw SQL query against the puzzle database.

    Args:
        query (str): The SQL query to execute.
        params (tuple, optional): The parameters to substitute into the query.

    Returns:
        list: A list of dictionaries representing the query result.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, params or ())
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
    db_type = get_database_type(conn)
    placeholder = "?" if db_type == "sqlite3" else "%s"

    cursor = conn.cursor()
    query = f'SELECT * FROM puzzles WHERE "PuzzleId" = {placeholder}'
    cursor.execute(query, (puzzle_id,))
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
    query = 'SELECT DISTINCT "Themes" FROM puzzles'
    cursor = conn.cursor()
    cursor.execute(query)
    
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
    query = 'SELECT MIN(CAST("Rating" AS INTEGER)), MAX(CAST("Rating" AS INTEGER)) FROM puzzles'
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchone()


def get_popularity_range():
    """
    Gets the minimum and maximum puzzle popularities in the database.

    Returns:
        tuple: (min_popularity, max_popularity).
    """
    conn = get_connection()
    query = 'SELECT MIN(CAST("Popularity" AS INTEGER)), MAX(CAST("Popularity" AS INTEGER)) FROM puzzles'
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchone()


def get_puzzle_attributes():
    """
    Retrieves the column names from the puzzles table.

    Returns:
        set: A set of attribute (column) names.
    """
    conn = get_connection()
    db_type = get_database_type(conn)
    cursor = conn.cursor()
    
    if db_type == "sqlite3":
        query = "PRAGMA table_info(puzzles)"
        cursor.execute(query)
        # The column name is in the second position (index 1)
        return {row[1] for row in cursor.fetchall()}
    elif db_type == "postgresql":
        query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'puzzles';"
        cursor.execute(query)
        return {row[0] for row in cursor.fetchall()}
    else:
        return set()


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
    if puzzles and not all(isinstance(puzzle, dict) for puzzle in puzzles):
        raise TypeError("Each item in the puzzles list must be a dictionary.")

    df = pd.DataFrame(puzzles)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8', header=header)
    except IOError as e:
        raise IOError(f"Error writing to file {file_path}: {e}")