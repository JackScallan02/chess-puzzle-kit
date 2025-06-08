import pytest
import pandas as pd
import sqlite3
from pathlib import Path
import chesspuzzlekit as cpk

# Define sample data for our test database
SAMPLE_PUZZLES = pd.DataFrame([
    {"PuzzleId": "00008", "FEN": "r6k/1p4bp/p1n1Q1p1/2p5/2P5/P3P3/1P1qN1PP/5RK1 b - - 2 23", "Moves": "d2e2 e6f7 e2e3 g1h1", "Rating": 1858, "RatingDeviation": 75, "Popularity": 97, "NbPlays": 1025, "Themes": "advancedPawn crushing defensiveMove endgame short", "GameUrl": "https://lichess.org/tI3r6y45/black#46", "OpeningTags": ""},
    {"PuzzleId": "0003b", "FEN": "r1b1k2r/1p1n1ppp/p2p1n2/q2Pp3/1b2P3/2N1BP2/PP1QN1PP/R3KB1R b KQkq - 3 10", "Moves": "f6e4 f3e4", "Rating": 917, "RatingDeviation": 91, "Popularity": 89, "NbPlays": 296, "Themes": "advantage hangingPiece middlegame oneMove", "GameUrl": "https://lichess.org/71zLcg31/black#20", "OpeningTags": "Sicilian_Defense Sicilian_Defense_Alapin_Variation"},
    {"PuzzleId": "0003h", "FEN": "rnb1kbnr/pp3ppp/8/2p1p3/2B1P3/8/PPP2PPP/RNBK2NR w kq - 0 6", "Moves": "c4f7 e8f7", "Rating": 629, "RatingDeviation": 100, "Popularity": 82, "NbPlays": 286, "Themes": "advantage hangingPiece opening oneMove", "GameUrl": "https://lichess.org/y4r9gPEU#11", "OpeningTags": "Caro-Kann_Defense Caro-Kann_Defense_Other_variations"},
    {"PuzzleId": "0004I", "FEN": "r4rk1/1p1q1ppp/p1p1p3/3n4/3P4/P1N1P3/1P1Q1PPP/R4RK1 w - - 0 16", "Moves": "c3d5 e6d5", "Rating": 1093, "RatingDeviation": 298, "Popularity": -33, "NbPlays": 4, "Themes": "advantage middlegame oneMove", "GameUrl": "https://lichess.org/D4PAgR1B#31", "OpeningTags": ""},
    {"PuzzleId": "0005D", "FEN": "r1bqkbnr/ppp2ppp/2np4/4p3/4P3/5N2/PPPPBPPP/RNBQ1RK1 b kq - 1 4", "Moves": "f7f5 e4f5", "Rating": 1000, "RatingDeviation": 84, "Popularity": 91, "NbPlays": 204, "Themes": "advantage oneMove opening", "GameUrl": "https://lichess.org/3e52p19b/black#8", "OpeningTags": "Philidor_Defense Philidor_Defense_Other_variations"},
    {"PuzzleId": "matey", "FEN": "5rk1/5ppp/R7/8/8/8/Pr3PPP/4R1K1 w - - 0 24", "Moves": "a6a8 f8a8", "Rating": 800, "RatingDeviation": 91, "Popularity": 91, "NbPlays": 600, "Themes": "hangingPiece mateIn2 endgame oneMove", "GameUrl": "https://lichess.org/yAb2u01B#47", "OpeningTags": ""},
])


@pytest.fixture(scope="module", autouse=True)
def test_db(tmp_path_factory):
    """
    Pytest fixture to create and configure a temporary SQLite database.

    Scoped to "module" to be created once per test module.
    Set to "autouse=True" to be automatically used by all tests,
    ensuring the database is set up without needing to pass it as an argument.
    """
    db_path = tmp_path_factory.mktemp("data") / "test_puzzles.db"
    conn = sqlite3.connect(db_path)
    
    # Populate the database with sample data
    SAMPLE_PUZZLES.to_sql("puzzles", conn, index=False, if_exists="replace")
    conn.close()
    
    # Initialize the connection in the ChessPuzzleKit package for the tests
    cpk.initialize_connection(db_path)
    
    yield db_path
    
    # Teardown: close any cached connections and reset global state after tests
    cpk.close_all_connections()
    cpk.set_db_path(None)


@pytest.fixture(scope="module")
def sample_puzzles_df():
    """A fixture that provides the raw sample puzzles DataFrame."""
    return SAMPLE_PUZZLES