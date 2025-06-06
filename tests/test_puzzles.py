import pytest
import pandas as pd
import sqlite3
from pathlib import Path

# Import the entire package with an alias
import ChessPuzzleKit as cpk

# Define sample data for our test database
SAMPLE_PUZZLES = pd.DataFrame([
    {"PuzzleId": "00008", "FEN": "r6k/1p4bp/p1n1Q1p1/2p5/2P5/P3P3/1P1qN1PP/5RK1 b - - 2 23", "Moves": "d2e2 e6f7 e2e3 g1h1", "Rating": 1858, "RatingDeviation": 75, "Popularity": 97, "NbPlays": 1025, "Themes": "advancedPawn crushing defensiveMove endgame short", "GameUrl": "https://lichess.org/tI3r6y45/black#46", "OpeningTags": ""},
    {"PuzzleId": "0003b", "FEN": "r1b1k2r/1p1n1ppp/p2p1n2/q2Pp3/1b2P3/2N1BP2/PP1QN1PP/R3KB1R b KQkq - 3 10", "Moves": "f6e4 f3e4", "Rating": 917, "RatingDeviation": 91, "Popularity": 89, "NbPlays": 296, "Themes": "advantage hangingPiece middlegame oneMove", "GameUrl": "https://lichess.org/71zLcg31/black#20", "OpeningTags": "Sicilian_Defense Sicilian_Defense_Alapin_Variation"},
    {"PuzzleId": "0003h", "FEN": "rnb1kbnr/pp3ppp/8/2p1p3/2B1P3/8/PPP2PPP/RNBK2NR w kq - 0 6", "Moves": "c4f7 e8f7", "Rating": 629, "RatingDeviation": 100, "Popularity": 82, "NbPlays": 286, "Themes": "advantage hangingPiece opening oneMove", "GameUrl": "https://lichess.org/y4r9gPEU#11", "OpeningTags": "Caro-Kann_Defense Caro-Kann_Defense_Other_variations"},
    {"PuzzleId": "0004I", "FEN": "r4rk1/1p1q1ppp/p1p1p3/3n4/3P4/P1N1P3/1P1Q1PPP/R4RK1 w - - 0 16", "Moves": "c3d5 e6d5", "Rating": 1093, "RatingDeviation": 298, "Popularity": -33, "NbPlays": 4, "Themes": "advantage middlegame oneMove", "GameUrl": "https://lichess.org/D4PAgR1B#31", "OpeningTags": ""},
    {"PuzzleId": "0005D", "FEN": "r1bqkbnr/ppp2ppp/2np4/4p3/4P3/5N2/PPPPBPPP/RNBQ1RK1 b kq - 1 4", "Moves": "f7f5 e4f5", "Rating": 1000, "RatingDeviation": 84, "Popularity": 91, "NbPlays": 204, "Themes": "advantage oneMove opening", "GameUrl": "https://lichess.org/3e52p19b/black#8", "OpeningTags": "Philidor_Defense Philidor_Defense_Other_variations"},
    {"PuzzleId": "matey", "FEN": "5rk1/5ppp/R7/8/8/8/Pr3PPP/4R1K1 w - - 0 24", "Moves": "a6a8 f8a8", "Rating": 800, "RatingDeviation": 91, "Popularity": 91, "NbPlays": 600, "Themes": "hangingPiece mateIn2 endgame oneMove", "GameUrl": "https://lichess.org/yAb2u01B#47", "OpeningTags": ""},
])


@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    """
    Pytest fixture to create a temporary, populated SQLite database for testing.
    This fixture is scoped to the module, so the DB is created only once.
    """
    db_path = tmp_path_factory.mktemp("data") / "test_puzzles.db"
    conn = sqlite3.connect(db_path)
    
    # Use pandas to easily insert our sample data into the 'puzzles' table
    SAMPLE_PUZZLES.to_sql("puzzles", conn, index=False, if_exists="replace")
    
    conn.close()
    
    # Yield the path to the tests
    yield db_path
    
    # Teardown: close any cached connections after all tests in the module run
    # Use the cpk alias here as well
    cpk.close_all_connections()


def test_get_all_themes(test_db):
    cpk.set_db_path(test_db)
    themes = cpk.get_all_themes()
    assert isinstance(themes, set)
    assert "mateIn2" in themes
    assert "crushing" in themes


def test_get_puzzle_by_id(test_db):
    cpk.set_db_path(test_db)
    puzzle_id = '00008'
    puzzle = cpk.get_puzzle_by_id(puzzle_id)
    assert isinstance(puzzle, dict)
    assert puzzle["PuzzleId"] == puzzle_id
    assert "FEN" in puzzle


def test_get_puzzle_by_invalid_id(test_db):
    cpk.set_db_path(test_db)
    puzzle = cpk.get_puzzle_by_id("non_existent_id")
    assert puzzle is None


def test_get_puzzle_raw(test_db):
    cpk.set_db_path(test_db)
    query = "SELECT * FROM puzzles WHERE Themes LIKE ? LIMIT ?"
    params = ('%mateIn2%', 1)
    result = cpk.get_puzzle_raw(query, params)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "mateIn2" in result[0]["Themes"]


def test_get_puzzle(test_db):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle()
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle


def test_get_puzzle_with_rating_range(test_db):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle(ratingRange=(600, 700))
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert 600 <= puzzle["Rating"] <= 700


def test_get_puzzle_with_popularity_range(test_db):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle(popularityRange=(90, 95))
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert 90 <= puzzle["Popularity"] <= 95


def test_get_puzzle_with_themes(test_db):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle(themes=["mateIn2"])
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert "mateIn2" in puzzle["Themes"]
    assert puzzle["PuzzleId"] == "matey"


def test_get_puzzle_with_count(test_db):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle(count=5)
    assert isinstance(result, list)
    assert len(result) == 5
    for puzzle in result:
        assert isinstance(puzzle, dict)
        assert "PuzzleId" in puzzle


def test_get_puzzle_with_invalid_count(test_db):
    cpk.set_db_path(test_db)
    with pytest.raises(ValueError, match="Count must be a positive integer"):
        cpk.get_puzzle(count=-1)

    with pytest.raises(ValueError, match="Count must be a positive integer"):
        cpk.get_puzzle(count=0)


def test_write_puzzles_to_file(test_db, tmp_path):
    cpk.set_db_path(test_db)
    result = cpk.get_puzzle(count=3)
    file_path = tmp_path / "test_puzzles.csv"
    cpk.write_puzzles_to_file(result, file_path)
    assert file_path.exists()
    # Check content
    df = pd.read_csv(file_path)
    assert len(df) == 3
    assert "PuzzleId" in df.columns


def test_get_rating_range(test_db):
    cpk.set_db_path(test_db)
    min_rating, max_rating = cpk.get_rating_range()
    assert min_rating == 629
    assert max_rating == 1858


def test_get_popularity_range(test_db):
    cpk.set_db_path(test_db)
    min_popularity, max_popularity = cpk.get_popularity_range()
    assert min_popularity == -33
    assert max_popularity == 97


def test_get_puzzle_attributes(test_db):
    cpk.set_db_path(test_db)
    attributes = cpk.get_puzzle_attributes()
    assert isinstance(attributes, set)
    # Check against the columns from our sample data
    expected_attributes = set(SAMPLE_PUZZLES.columns)
    assert attributes == expected_attributes