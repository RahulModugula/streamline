"""
pytest configuration for Streamline tests.

Adds spark/ and producer/ to sys.path so tests can import modules without
hard-coding absolute paths. This file is loaded automatically by pytest
before any test file is executed.
"""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for _dir in ("spark", "producer"):
    _path = os.path.join(_root, _dir)
    if _path not in sys.path:
        sys.path.insert(0, _path)
