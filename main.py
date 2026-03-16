#!/usr/bin/env python3
"""
Main entry point for the Python application.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from app import main as app_main


if __name__ == "__main__":
    app_main()
