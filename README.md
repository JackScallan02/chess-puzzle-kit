# ChessPuzzleKit

**ChessPuzzleKit** is a Python library for accessing and working with chess puzzles from the [Lichess puzzle database](https://database.lichess.org/#puzzles). It provides functions to query puzzles by theme, rating, and popularity, all with zero setup.

## Features

- Automatically downloads and caches the Lichess puzzle database
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

themes = cpk.get_all_themes()
puzzles = cpk.get_puzzle(themes=['fork'], count=3)

for p in puzzles:
    print(p['fen'], p['moves'], p['rating'])
```