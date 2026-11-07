"""Run: python -m streamlit run streamlit_app.py"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dashboard.app import main

main()
