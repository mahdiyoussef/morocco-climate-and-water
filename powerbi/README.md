# Morocco Climate & Water Observatory — Power BI

This report uses **Power BI Desktop**, alongside the detailed Streamlit companion. Both consume shared tested dbt Gold models. Tableau can also connect to the read-only PostgreSQL Gold schema.

Open **[Morocco.pbip](Morocco.pbip)** in Power BI Desktop and choose **Refresh**. The native project contains its semantic model, Power Query imports, measures, theme, and four report pages. Its CSV sources are already generated in `data/gold/`. If the project moves, change the `DataFolder` parameter to the new Gold folder. Power BI Desktop rendering and DAX execution have not been verified in this environment; JSON schemas and model bindings are checked separately.

The first stage downloads and validates CHIRPS monthly source archives for 2015–2024. The minimum size gate is **1,073,741,825 bytes**, which is strictly greater than 1 GiB. It counts completed climate input files only. Software, documentation, partial downloads and OpenStreetMap files do not count.

CHIRPS annual NetCDF source archives cover the globe. They will be clipped to the Morocco Geofabrik extract footprint. The source archive size must not be represented as the size of a Morocco-only subset.

After climate acquisition, run the regional processing pipeline to combine CHIRPS, explicitly selected ERA5-Land observations from Open-Meteo, and OpenStreetMap context. The output model is:

| Table | Grain | Relationships |
| --- | --- | --- |
| `fact_climate_monthly.csv` | Location × year × month | `site` to `dim_location.id`; `year_month` to `dim_month.year_month` |
| `dim_location.csv` | Twelve sample locations | One side of climate and infrastructure relationships |
| `dim_month.csv` | Calendar month | One side of climate relationship |
| `fact_infrastructure.csv` | Location, contemporary OSM snapshot | `site` to `dim_location.id` |

Use single-direction relationships from dimensions to facts. Keep `year_month` as Text on both sides. Set location `lat` and `lon` to Decimal Number and their data categories to Latitude and Longitude. Climate measurements represent nearby grid cells, not complete cities or national means. Infrastructure is a contemporary mapped snapshot, not an annual climate observation.

The semantic model also imports the four dbt analytical marts: seasonal departures, water years, location trends and footprint rainfall. See the [platform runbook](../docs/morocco-platform.md) for model grains, PostgreSQL access and interpretation.

Included report pages:

1. **Climate overview** — year and month filters; rainfall, temperature, soil moisture, location map, monthly trend, source coverage.
2. **Location comparison** — rain anomaly, temperature and moisture by sampling location; mapped water, education and healthcare context.
3. **dbt water & climate insights** - short-reference seasonal departures, complete water years, full-footprint rainfall and descriptive slopes.
4. **Sources & methodology** — input sizes, joined row counts, file checksums, source attribution and analysis limitations.

Power BI imports the small Gold tables, while Spark and dbt process the larger source data outside the report. Raw NetCDF files are not loaded directly into report visuals.

## Connect Power BI Desktop

The supplied `.pbip` already includes these imports. To rebuild the model manually from the editable query assets:

1. In **Transform data**, create a blank query named `DataFolder` and paste `queries/DataFolder.pq` into its Advanced Editor. Change the path if the project moved.
2. Create the nine queries from the matching files in `queries/`: ClimateMonthly, Locations, Calendar, Infrastructure, SourceAudit, SeasonalDepartures, WaterYears, LocationTrends and FootprintRainfall. The supplied native semantic model already defines their relationships.
3. Close and apply; create the relationships listed above. In the model, sort `Calendar.month_name` by `Calendar.month`.
4. Add the expressions in `measures.dax` as individual measures. Format `Rainfall Change Percent` as Percentage; `Surface Soil Moisture Percent` is already multiplied by 100 and should use a numeric format.
5. Import `theme.json` using **View → Themes → Browse for themes**. Use a single-select year slicer and default it to 2024. Add the report pages listed above.

The `.pq`, `.dax` and theme files are also provided as editable assets. The report is supplied in native **PBIP/PBIR** project format, rather than as a binary `.pbix`. Open it in Desktop, refresh, and save as `.pbix` if needed. The Azure Maps visual requires Azure Maps to be enabled in Desktop or by the tenant administrator. Other report pages do not depend on maps.

## Regenerate and validate the native project

```powershell
./.tools/python/python.exe observatory/build_powerbi.py
./.tools/python/python.exe observatory/validate_powerbi.py
```

The validator uses Microsoft's public PBIR schemas, checks visual field references and relationships, and verifies dimension-key uniqueness. It does not execute DAX or render the report. See `validation.json` for the recorded verification limit.
