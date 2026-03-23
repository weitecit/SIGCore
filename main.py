#!/usr/bin/env python3
"""
Main entry point for the Python application.
"""

import sys
from pathlib import Path
import uvicorn

sys.path.insert(0, str(Path(__file__).parent / "src"))

from api import app

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
