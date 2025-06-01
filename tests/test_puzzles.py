import ChessPuzzleKit as cpk
from pathlib import Path

def test_get_all_themes():
    themes = cpk.get_all_themes()
    assert isinstance(themes, set)
    assert len(themes) > 0
    assert "mateIn2" in themes

def test_get_puzzle_by_id():
    puzzle_id = '00008'
    puzzle = cpk.get_puzzle_by_id(puzzle_id)
    print("Puzzle:", puzzle)
    assert isinstance(puzzle, dict)
    assert puzzle["PuzzleId"] == puzzle_id
    assert "FEN" in puzzle

def test_get_puzzle_by_invalid_id():
    puzzle_id = "9999999999999999"
    puzzle = cpk.get_puzzle_by_id(puzzle_id)
    assert puzzle is None

def test_get_puzzle_raw():
    query = "SELECT * FROM puzzles WHERE Themes LIKE '%mateIn2%' LIMIT 5"
    puzzles = cpk.get_puzzle_raw(query)
    assert isinstance(puzzles, list)
    assert len(puzzles) > 0
    for puzzle in puzzles:
        assert "PuzzleId" in puzzle
        assert "FEN" in puzzle

def test_get_puzzle():
    puzzle = cpk.get_puzzle()
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle
    assert "FEN" in puzzle

def test_get_puzzle_with_rating_range():
    puzzle = cpk.get_puzzle(ratingRange=(1500, 2000))
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle
    assert "FEN" in puzzle
    assert 1500 <= puzzle["Rating"] <= 2000

def test_get_puzzle_with_popularity_range():
    puzzle = cpk.get_puzzle(popularityRange=(100, 500))
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle
    assert "FEN" in puzzle
    assert 100 <= puzzle["Popularity"] <= 500

def test_get_puzzle_with_themes():
    puzzle = cpk.get_puzzle(themes=["mateIn2"])
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle
    assert "FEN" in puzzle
    assert "mateIn2" in puzzle["Themes"]

def test_get_puzzle_with_multiple_themes():
    puzzle = cpk.get_puzzle(themes=["mateIn2", "tactics"])
    assert isinstance(puzzle, dict)
    assert "PuzzleId" in puzzle
    assert "FEN" in puzzle
    assert "mateIn2" in puzzle["Themes"] or "tactics" in puzzle["Themes"]

def test_get_puzzle_with_count():
    puzzles = cpk.get_puzzle(count=5)
    assert isinstance(puzzles, list)
    assert len(puzzles) == 5
    for puzzle in puzzles:
        assert isinstance(puzzle, dict)
        assert "PuzzleId" in puzzle
        assert "FEN" in puzzle

def test_get_puzzle_with_invalid_count():
    try:
        cpk.get_puzzle(count=-1)
        assert False, "Expected ValueError for negative count"
    except ValueError as e:
        assert str(e) == "Count must be a positive integer."

    try:
        cpk.get_puzzle(count=0)
        assert False, "Expected ValueError for zero count"
    except ValueError as e:
        assert str(e) == "Count must be a positive integer."


def test_write_puzzles_to_file():
    puzzles = cpk.get_puzzle(count=5)
    file_path = "test_puzzles.json"
    cpk.write_puzzles_to_file(puzzles, file_path)
    
    assert Path(file_path).exists()
    Path(file_path).unlink(missing_ok=True)

def test_get_rating_range():
    min_rating, max_rating = cpk.get_rating_range()
    assert isinstance(min_rating, int)
    assert isinstance(max_rating, int)
    assert min_rating <= max_rating

def test_get_popularity_range():
    min_popularity, max_popularity = cpk.get_popularity_range()
    assert isinstance(min_popularity, int)
    assert isinstance(max_popularity, int)
    assert min_popularity <= max_popularity

def test_get_puzzle_attributes():
    attributes = cpk.get_puzzle_attributes()
    assert isinstance(attributes, set)
    assert len(attributes) > 0
    assert "FEN" in attributes
    assert "Rating" in attributes


