# AtlasPulse Streamlit dashboard

Seven analytical sections use the same Gold fact tables and dimensions as Power BI, plus four dbt analytical marts. Measurements come from downloaded CHIRPS, ERA5-Land and OSM sources; there is no synthetic fallback.

```powershell
python -m pip install -r dashboard/requirements.txt
python -m streamlit run streamlit_app.py
```

Open http://localhost:8501. In this workspace, `./.tools/python/python.exe` can replace `python`. Local Gold exports are included; rebuild with `python observatory/warehouse.py local-all` after installing `observatory/requirements-platform-local.txt`.

| Section | Contents |
| --- | --- |
| Overview | Scoped KPIs, climate map, seasonal comparison and attribution |
| Indicators | 24 definitions, complete-year rainfall variability and concentration, and reference-condition frequencies |
| Climate explorer | Gapped timelines, heatmaps, reference-year departures, distributions and grid coordinates |
| Compare locations | Rankings, rainfall/temperature scatter, infrastructure context and comparison export |
| Water & climate insights | dbt seasonal departures, water years, footprint rainfall and exploratory slopes |
| Infrastructure | Actual mapped objects, categories, 20 km context and GeoJSON export |
| Data & methodology | Source volume, coverage, checks, dbt results, inventory and formulas |

Filters control locations, years, months, a matching-month reference year and optional online map tiles. Water years and full-history trends state their filter exceptions. The dbt reference (2015–2019) is distinct from the sidebar single-year comparison. Period rainfall is averaged across location-years; land variables are hour-weighted within each location. Zero rainfall baselines yield undefined percentages. Empty selections show a recovery message.

`OBSERVATORY_DATA_ROOT` may point to a directory containing `gold/` and optional `metadata/`. Default storage is local. The Morocco Compose stack uses `OBSERVATORY_DATA_BACKEND=minio`, follows a committed private snapshot, verifies object checksums, and optionally caches its pointer in Redis. Credentials stay on the Python server.

Run `python -m pytest tests` for numerical and AppTest coverage. Browser map rendering and Power BI Desktop rendering are separate checks. See the [platform runbook](../docs/morocco-platform.md).

For the self-contained Community Cloud version, see [`../prod/README.md`](../prod/README.md). It uses bundled Gold files and has no database dependency. Refresh with `observatory/analyze_indicators.py` and `observatory/build_prod.py`.
