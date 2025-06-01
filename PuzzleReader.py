import random
import re
import pandas as pd

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
        reader = PuzzleReader(puzzle_file='path/to/your/lichess_db_puzzle.csv')
        puzzles = reader.get_puzzle(themes=['mateIn2', 'mateIn3'], ratingRange=[1000, 2000], popularityRange=[80, 100], count=20)
        print(puzzles)


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

    def __init__(self, puzzle_file):
        self.puzzle = []

        if (puzzle_file):
            self.read_puzzle_file(puzzle_file)

    def read_puzzle_file(self, puzzle_file):
        """
        Reads the CSV file containing lichess puzzles and stores it in a pandas DataFrame.

        Args:
            puzzle_file (str): The path to the CSV file containing the puzzles.

        Returns:
            pd.DataFrame: A DataFrame containing the puzzles read from the CSV file.

        Raises:
            FileNotFoundError: If the specified file does not exist.
            pd.errors.EmptyDataError: If the file is empty.
            Exception: For any other errors that occur while reading the file.
        """
        try:
            self.puzzle = pd.read_csv(puzzle_file, low_memory=False)
            print(f"Successfully read {len(self.puzzle)} puzzles from {puzzle_file}")
        except FileNotFoundError:
            print(f"Error: The file {puzzle_file} does not exist.")
        except pd.errors.EmptyDataError:
            print("Error: The file is empty.")
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")

        return self.puzzle
    
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
        if type(puzzle_id) is not int:
            raise TypeError("PuzzleId must be an integer.")

        puzzle = self.puzzle[self.puzzle['PuzzleId'] == puzzle_id]
        if not puzzle.empty:
            return puzzle.to_dict(orient='records')[0]
        else:
            return None

    def get_puzzle(self, themes=None, ratingRange=None, popularityRange=None, count=10):
        """
        Returns a random list of puzzles.

        Args
            themes (list, optional): A list of themes to filter puzzles by.
            ratingRange (list | tuple, optional): Can be used to filter puzzles by a range of ratings (339, 3352).
            popularityRange (list | tuple, optional): Can be used to filter puzzles by range of popularity (-89, 100).
            count (int, optional): The number of puzzles to return (default 10).

        Returns:
            List[Dict]: A list of dictionaries representing the filtered puzzles.

        Raises:
            TypeError: If themes is not a list, count is not an integer, or ratingRange/popularityRange is not a list/tuple of 2 integers.
            ValueError: If one or more themes are not found, or if count exceeds the number of puzzles available.
        """
        if type(themes) is not list and themes is not None:
            raise TypeError("Themes must be a list.")
        elif type(count) is not int:
            raise TypeError("Count must be an integer.")
        elif ratingRange is not None and ((type(ratingRange) is not list and type(ratingRange) is not tuple) or len(ratingRange) != 2) or not all(isinstance(x, int) for x in ratingRange):
            raise TypeError("Rating range must be a list or tuple of 2 integers.")
        elif popularityRange is not None and ((type(popularityRange) is not list and type(popularityRange) is not tuple) or len(popularityRange) != 2) or not all(isinstance(x, int) for x in popularityRange):
            raise TypeError("Popularity range must be a list or tuple of 2 integers.")
        elif themes is not None and any(theme not in self.get_all_themes() for theme in themes):
            raise ValueError(f"One or more themes not found. Available themes: {self.get_all_themes()}")
        elif count > len(self.puzzle):
            raise ValueError(f"Count {count} exceeds the number of puzzles available: {len(self.puzzle)}")

        filtered_df = self.puzzle
        if ratingRange is not None:
            filtered_df = self.puzzle[(self.puzzle['Rating'] >= ratingRange[0]) & (self.puzzle['Rating'] <= ratingRange[1])]

        if themes is not None:
            pattern = '|'.join(map(re.escape, themes))
            filtered_df = filtered_df[filtered_df['Themes'].str.contains(pattern, case=False, na=False)]

        if popularityRange is not None:
            filtered_df = self.puzzle[(self.puzzle['Popularity'] >= popularityRange[0]) & (self.puzzle['Popularity'] <= popularityRange[1])]

        random_rows = filtered_df.sample(n=count, random_state=random.randint(0, 1000))
        return random_rows.to_dict(orient='records')
    
    def get_all_themes(self):
        """
        Retrieves a list of all unique themes in the csv file.

        Returns:
            set: A set of all unique themes found in the puzzles.

        """
        listOfThemes = self.puzzle['Themes'].unique().tolist()
        setOfThemes = set()
        for theme in listOfThemes:
            splitThemes = theme.split(' ')
            setOfThemes.update(splitThemes)

        return setOfThemes
    
    def get_rating_range(self):
        """
        Retrieves the minimum and maximum ratings from the puzzles.
        Returns:
            tuple: A tuple containing the minimum and maximum ratings
        """
        if self.puzzle.empty:
            return None, None
        return self.puzzle['Rating'].min(), self.puzzle['Rating'].max()
    
    def get_popularity_range(self):
        """
        Retrieves the minimum and maximum popularity from the puzzles.
        Returns:
            tuple: A tuple containing the minimum and maximum popularity
        """
        if self.puzzle.empty:
            return None, None
        return self.puzzle['Popularity'].min(), self.puzzle['Popularity'].max()
    
    def get_csv_attributes(self):
        """
        Retrieves all column attributes of the CSV file as a list.

        Returns:
            list: A list of column names in the CSV file.
        """
        if self.puzzle.empty:
            return []
        return self.puzzle.columns.tolist()


if __name__ == "__main__":
    reader = PuzzleReader(puzzle_file='../lichess_db_puzzle.csv')
    print(reader.get_csv_attributes())
    print(reader.get_popularity_range())
    print(reader.get_rating_range())
    print(reader.get_puzzle(themes=['mateIn2', 'mateIn3'], ratingRange=[1000, 2000], popularityRange=[80, 100], count=20))