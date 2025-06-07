# ChessPuzzleKit
[![PyPI version](https://img.shields.io/pypi/v/ChessPuzzleKit.svg)](https://pypi.org/project/ChessPuzzleKit/)

**ChessPuzzleKit** is a Python library for accessing and working with chess puzzles from the [Lichess puzzle database](https://database.lichess.org/#puzzles). It provides functionality to retrieve unique puzzles by theme types, rating, and popularity, all with zero setup.

## Features

- Automatically downloads and caches a Lichess puzzle database (almost 5 million puzzles, ~900 MB)
- Filter puzzles by:
  - Theme (e.g. `fork`, `pin`, `mateIn2`, etc.)
  - Rating or popularity range
- Returns puzzles as Python dictionaries

## Installation
You can install this package from [PyPI](https://pypi.org/project/ChessPuzzleKit/):
```bash
pip install ChessPuzzleKit
```

## Usage
```py
import ChessPuzzleKit as cpk

# Optional: use custom sqlite database or CSV file
custom_db_path = cpk.create_db_csv("/path/to/local_csv_file.csv")
cpk.set_db_path(custom_db_path)

# Or, locally download the database if it doesn't exist
# By default, the database gets downloaded to .chess_puzzles in the HOME directory
cpk.get_connnection()

puzzles = cpk.get_puzzle(themes=['fork'], ratingRange=[2000, 2200], count=3)
themes = cpk.get_all_themes()
print(themes)

for p in puzzles:
    print(p['fen'], p['moves'], p['rating'])
```

### Supported Puzzle Themes

```text
attackingF2F7      queensideAttack      kingsideAttack      middlegame
quietMove          advancedPawn         promotion           underPromotion
enPassant          interference         deflection          intermezzo
clearance          attraction           discoveredAttack    xRayAttack
skewer             fork                 pin                 doubleCheck
sacrifice          trappedPiece         hangingPiece        defensiveMove
equality           endgame              pawnEndgame         rookEndgame
bishopEndgame      knightEndgame        queenEndgame        queenRookEndgame
capturingDefender  zugzwang             mateIn1             mateIn2
mateIn3            mateIn4              mateIn5             mate
backRankMate       smotheredMate        bodenMate           anastasiaMate
doubleBishopMate   arabianMate          hookMate            killBoxMate
vukovicMate        dovetailMate         exposedKing         crushing
veryLong           long                 short               oneMove
master             superGM              masterVsMaster      advantage
opening            castling
```

### Available Puzzle Attributes

| Attribute         | Description (editable)                              |
|------------------|------------------------------------------------------|
| `GameUrl`         | Link to the full Lichess game                       |
| `FEN`             | Board state in Forsyth–Edwards Notation             |
| `Moves`           | Solution moves for the puzzle                       |
| `OpeningTags`     | Opening tags or names                               |
| `PuzzleId`        | Unique identifier for the puzzle                    |
| `NbPlays`         | Number of times the puzzle has been played          |
| `Popularity`      | Popularity score of the puzzle (-89 to 100)         |
| `Rating`          | Difficulty rating of the puzzle (339 to 3352)       |
| `RatingDeviation` | Uncertainty in the puzzle's rating                  |
| `Themes`          | List of themes (e.g., fork, pin, mateIn2)           |

### Contributions
Bug reports, bug fixes, documentation improvements, enhancements, and ideas are all welcome - please submit an issue or start a discussion in this repository. For anything else, please send inquiries to `breezechess99@gmail.com`.
