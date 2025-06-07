import pytest
import pandas as pd
from pathlib import Path
import ChessPuzzleKit as cpk

def test_get_all_themes():
    themes = cpk.get_all_themes()
    assert isinstance(themes, set)
    assert "mateIn2" in themes
    assert "crushing" in themes


def test_get_puzzle_by_id():
    puzzle_id = '00008'
    puzzle = cpk.get_puzzle_by_id(puzzle_id)
    assert isinstance(puzzle, dict)
    assert puzzle["PuzzleId"] == puzzle_id
    assert "FEN" in puzzle


def test_get_puzzle_by_invalid_id():
    puzzle = cpk.get_puzzle_by_id("non_existent_id")
    assert puzzle is None


def test_get_puzzle_raw():
    # This test demonstrates usage for SQLite, as that's what the test fixture uses.
    # The user of get_puzzle_raw is responsible for correct syntax per DB.
    query = "SELECT * FROM puzzles WHERE Themes LIKE ? LIMIT ?"
    params = ('%mateIn2%', 1)
    result = cpk.get_puzzle_raw(query, params)
    assert isinstance(result, list)
    assert len(result) == 1
    assert "mateIn2" in result[0]["Themes"]


def test_get_puzzle():
    result = cpk.get_puzzle()
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle


def test_get_puzzle_with_rating_range():
    # Test a narrow range to ensure filtering works
    result = cpk.get_puzzle(ratingRange=(600, 700), count=1)
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert 600 <= int(puzzle["Rating"]) <= 700


def test_get_puzzle_with_popularity_range():
    result = cpk.get_puzzle(popularityRange=(95, 100), count=1)
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert 95 <= int(puzzle["Popularity"]) <= 100
    assert puzzle['PuzzleId'] == '00008'


def test_get_puzzle_with_themes():
    result = cpk.get_puzzle(themes=["mateIn2"])
    assert isinstance(result, list)
    assert len(result) == 1
    puzzle = result[0]
    assert "mateIn2" in puzzle["Themes"]
    assert puzzle["PuzzleId"] == "matey"


def test_get_puzzle_with_count():
    result = cpk.get_puzzle(count=5)
    assert isinstance(result, list)
    assert len(result) == 5
    for puzzle in result:
        assert isinstance(puzzle, dict)
        assert "PuzzleId" in puzzle


def test_get_puzzle_with_invalid_count():
    with pytest.raises(ValueError, match="Count must be a positive integer"):
        cpk.get_puzzle(count=-1)

    with pytest.raises(ValueError, match="Count must be a positive integer"):
        cpk.get_puzzle(count=0)


def test_write_puzzles_to_file(tmp_path):
    result = cpk.get_puzzle(count=3)
    file_path = tmp_path / "test_puzzles.csv"
    cpk.write_puzzles_to_file(result, file_path)
    assert file_path.exists()
    df = pd.read_csv(file_path)
    assert len(df) == 3
    assert "PuzzleId" in df.columns


def test_get_rating_range():
    min_rating, max_rating = cpk.get_rating_range()
    assert min_rating == 629
    assert max_rating == 1858


def test_get_popularity_range():
    min_popularity, max_popularity = cpk.get_popularity_range()
    assert min_popularity == -33
    assert max_popularity == 97


def test_get_puzzle_attributes(sample_puzzles_df):
    attributes = cpk.get_puzzle_attributes()
    assert isinstance(attributes, set)
    expected_attributes = set(sample_puzzles_df.columns)
    assert attributes == expected_attributes