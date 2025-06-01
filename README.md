# ChessPuzzleKit

**ChessPuzzleKit** is a Python library for accessing and working with chess puzzles from the [Lichess puzzle database](https://database.lichess.org/#puzzles). It provides functionality to retrieve unique puzzles by theme types, rating, and popularity, all with zero setup.

## Features

- Automatically downloads and caches a Lichess puzzle database (almost 5 million puzzles, ~900 MB)
- Filter puzzles by:
  - Theme (e.g. `fork`, `pin`, `mateIn2`, etc.)
  - Rating or popularity range
- Returns puzzles as Python dictionaries

## Installation

```bash
pip install ChessPuzzleKit
```

## Usage
```py
import ChessPuzzleKit as cpk

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

### Rating and Popularity Ranges

| Attribute    | Min   | Max   |
|--------------|-------|-------|
| Rating       | 339   | 3352  |
| Popularity   | -89   | 100   |
