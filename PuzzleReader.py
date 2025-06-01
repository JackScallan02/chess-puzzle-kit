import pandas as pd
import sqlite3
from pathlib import Path
import os
import requests

class PuzzleReader:
    """
    This class manages reading and filtering lichess puzzles from a CSV file.
    The class provides methods to filter puzzles by themes, retrieve random puzzles,
    and write puzzles to a file.

    By default, this class reads the CSV file 'lichess_db_puzzle.csv' from https://database.lichess.org/#puzzles
    which contains 4,827,507 chess puzzles, last updated on 2025-05-16.

    Args:
        puzzle_file (str): The path to the CSV file containing the puzzles. If not provided, it defaults to 'lichess_db_puzzle.csv'.
    Attributes:
        puzzle (pd.DataFrame): A pandas DataFrame containing the puzzles read from the CSV file.
    Usage:
    >>> reader = PuzzleReader()
    >>> puzzles = reader.get_puzzle(themes=['mateIn1'], ratingRange=[1000, 2000], count=5)
    >>> reader.write_puzzles_to_file(puzzles, 'filtered_puzzles.csv')
    >>> reader.get_puzzle_by_id(123456)
    >>> reader.get_all_themes()
    >>> reader.get_rating_range()
    >>> reader.get_popularity_range()
    >>> reader.get_puzzle_attributes()

    The CSV file contains the following columns:
    ['PuzzleId', 'FEN', 'Moves', 'Rating', 'RatingDeviation', 'Popularity', 'NbPlays', 'Themes', 'GameUrl', 'OpeningTags']
        - PuzzleId: Unique identifier for the puzzle.
        - FEN: Forsyth-Edwards Notation string representing the chess board position.
        - Moves: The moves leading to the solution of the puzzle.
        - Rating: The puzzle's rating.
        - RatingDeviation: The deviation of the puzzle's rating.
        - Popularity: The popularity of the puzzle.
        - NbPlays: The number of times the puzzle has been played.
        - Themes: A string containing the themes associated with the puzzle.
        - GameUrl: The Lichess website URL of the game from which the puzzle was derived.
        - OpeningTags: Tags related to the opening of the game.

    Possible themes are:
    'attackingF2F7', 'queensideAttack', 'kingsideAttack', 'middlegame', 'quietMove', 'advancedPawn',
    'promotion', 'underPromotion', 'enPassant', 'interference', 'deflection', 'intermezzo', 'clearance',
    'attraction', 'discoveredAttack', 'xRayAttack', 'skewer', 'fork', 'pin', 'doubleCheck', 'sacrifice',
    'trappedPiece', 'hangingPiece', 'defensiveMove', 'equality', 'endgame', 'pawnEndgame', 'rookEndgame',
    'bishopEndgame', 'knightEndgame', 'queenEndgame', 'queenRookEndgame', 'capturingDefender', 'zugzwang',
    'mateIn1', 'mateIn2', 'mateIn3', 'mateIn4', 'mateIn5', 'mate', 'backRankMate', 'smotheredMate',
    'bodenMate', 'anastasiaMate', 'doubleBishopMate', 'arabianMate', 'hookMate', 'killBoxMate', 'vukovicMate',
    'dovetailMate', 'exposedKing', 'crushing', 'deflection', 'veryLong', 'long', 'short', 'oneMove', 'master',
    'superGM', 'masterVsMaster', 'advantage', 'opening', 'castling'


    Possible ratings range from 339 to 3352.
    Possible popularities range from -89 to 100.

    """

    DB_URL = 'https://github.com/JackScallan02/chess-puzzle-kit/releases/download/v0.1.0/lichess_db_puzzle.db'
    LOCAL_PATH = Path.home() / '.chess_puzzles' / 'lichess_db_puzzle.db'

    def __init__(self):
        self.puzzle = []

        if not self.LOCAL_PATH.exists():
            self._download_db()
        self.conn = sqlite3.connect(self.LOCAL_PATH)

    def _download_db(self):
        os.makedirs(self.LOCAL_PATH.parent, exist_ok=True)
        print("Downloading puzzle database...")
        with requests.get(self.DB_URL, stream=True) as r:
            r.raise_for_status()
            with open(self.LOCAL_PATH, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def get_puzzle(self, themes=None, ratingRange=None, popularityRange=None, count=10):
        """
        Retrieves a list of random puzzles based on specified themes, rating range, and popularity range.
        Args:
            themes (list): A list of themes to filter puzzles by. If None, no theme filtering is applied.
            ratingRange (list): A list containing two integers [min_rating, max_rating] to filter puzzles by rating.
                                If None, no rating filtering is applied.
            popularityRange (list): A list containing two integers [min_popularity, max_popularity] to filter puzzles by popularity.
                                    If None, no popularity filtering is applied.
            count (int): The number of random puzzles to retrieve. Defaults to 10.
        Returns:
            list: A list of dictionaries, each representing a puzzle that matches the specified criteria.
                  Each dictionary contains the puzzle's attributes such as PuzzleId, FEN, Moves, Rating, etc.
        Raises:
            TypeError: If themes is not a list, or if ratingRange or popularityRange are not lists of two integers.
            ValueError: If count is not a positive integer.
        """
        if themes is not None and not isinstance(themes, list):
            raise TypeError("Themes must be a list.")
        if ratingRange is not None:
            if not isinstance(ratingRange, list) or len(ratingRange) != 2 or not all(isinstance(x, int) for x in ratingRange):
                raise TypeError("RatingRange must be a list of two integers.")
        if popularityRange is not None:
            if not isinstance(popularityRange, list) or len(popularityRange) != 2 or not all(isinstance(x, int) for x in popularityRange):
                raise TypeError("PopularityRange must be a list of two integers.")
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

        cursor = self.conn.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_puzzle_by_id(self, puzzle_id):
        """
        Retrieves a puzzle by its unique PuzzleId.

        Args:
            puzzle_id (int): The unique identifier for the puzzle.

        Returns:
            dict: A dictionary representing the puzzle with the specified PuzzleId, or None if not found.

        Raises:
            TypeError: If puzzle_id is not an integer.
        """
        if not isinstance(puzzle_id, int):
            raise TypeError("PuzzleId must be an integer.")
        
        query = "SELECT * FROM puzzles WHERE PuzzleId = ?"
        cursor = self.conn.execute(query, (puzzle_id,))
        row = cursor.fetchone()
        
        if row:
            columns = [col[0] for col in cursor.description]
            return dict(zip(columns, row))
        else:
            return None

    def get_all_themes(self):
        """
        Retrieves a list of all unique themes.

        Returns:
            set: A set of all unique themes found in the puzzles.

        """
        query = "SELECT DISTINCT Themes FROM puzzles"
        cursor = self.conn.execute(query)
        themes = set()
        for row in cursor.fetchall():
            if row[0]:
                themes.update(row[0].split(' '))
        return themes
    def write_puzzles_to_file(self, puzzles, file_path, header=False):
        """
        Writes a list of puzzles to a specified file in CSV format.
        Args:
            puzzles (list): A list of dictionaries, each representing a puzzle.
            file_path (str): The path to the file where the puzzles will be written.
            header (bool): If True, writes the header row to the CSV file. Defaults to False.
        Returns:
            None
        Raises:
            TypeError: If puzzles is not a list or if any puzzle is not a dictionary.
            IOError: If there is an error writing to the file.
        """
        if not isinstance(puzzles, list):
            raise TypeError("Puzzles must be a list.")
        if not all(isinstance(puzzle, dict) for puzzle in puzzles):
            raise TypeError("Each puzzle must be a dictionary.")
        
        df = pd.DataFrame(puzzles)
        try:
            df.to_csv(file_path, index=False, encoding='utf-8', header=False)
        except IOError as e:
            raise IOError(f"Error writing to file {file_path}: {e}")
    
    def get_rating_range(self):
        """
        Retrieves the minimum and maximum ratings from the puzzles.
        Returns:
            tuple: A tuple containing the minimum and maximum ratings
        """
        query = "SELECT MIN(Rating), MAX(Rating) FROM puzzles"
        cursor = self.conn.execute(query)
        min_rating, max_rating = cursor.fetchone()
        return (min_rating, max_rating)
    
    def get_popularity_range(self):
        """
        Retrieves the minimum and maximum popularity from the puzzles.
        Returns:
            tuple: A tuple containing the minimum and maximum popularity
        """
        query = "SELECT MIN(Popularity), MAX(Popularity) FROM puzzles"
        cursor = self.conn.execute(query)
        min_popularity, max_popularity = cursor.fetchone()
        return (min_popularity, max_popularity)
    
    def get_puzzle_attributes(self):
        """
        Retrieves all attributes of a puzzle object.

        Returns:
            set: A set of each column name in the database.
        """
        query = "PRAGMA table_info(puzzles)"
        cursor = self.conn.execute(query)
        attributes = {row[1] for row in cursor.fetchall()}
        return attributes


if __name__ == "__main__":
    reader = PuzzleReader()
    puzzles = reader.get_puzzle(themes=['mateIn1'], ratingRange=[1000, 2000], count=5)
    for puzzle in puzzles:
        print(puzzle)
    reader.write_puzzles_to_file(puzzles, 'filtered_puzzles.csv')
    print("Puzzles written to 'filtered_puzzles.csv' successfully.")

