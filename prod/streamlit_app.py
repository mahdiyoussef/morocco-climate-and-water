"""Standalone Gold snapshot app. Run: streamlit run streamlit_app.py"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from dashboard.app import main

# Explicit arguments keep this package independent of inherited database settings.
main(data_root=ROOT / "data", backend="local")
