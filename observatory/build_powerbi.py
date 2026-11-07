"""Generate a native Power BI project consuming the shared tested dbt Gold tables."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BI = ROOT / 'powerbi'
GOLD = ROOT / 'data/gold'
REPORT = BI / 'Observatory.Report'
MODEL = BI / 'Observatory.SemanticModel'
SCHEMA = 'https://developer.microsoft.com/json-schemas/fabric/item/report/'


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def field(table, column, measure=False):
    return {('Measure' if measure else 'Column'): {'Expression': {'SourceRef': {'Entity': table}}, 'Property': column}}


def projection(table, column, measure=False):
    return {'field': field(table, column, measure), 'queryRef': f'{table}.{column}'}


def literal(value):
    return {'expr': {'Literal': {'Value': value}}}


def visual(page, name, kind, rect, roles=None, title=None, objects=None):
    x, y, width, height = rect
    config = {'visualType': kind, 'drillFilterOtherVisuals': True}
    if roles:
        config['query'] = {'queryState': {key: {'projections': values} for key, values in roles.items()}}
    if title:
        config['visualContainerObjects'] = {'title': [{'properties': {
            'show': literal('true'), 'text': literal("'" + title.replace("'", "''") + "'"),
            'fontColor': {'solid': {'color': literal("'#19382E'")}}, 'fontSize': literal('11D')}}]}
    if objects:
        config['objects'] = objects
    doc = {'$schema': SCHEMA + 'definition/visualContainer/1.0.0/schema.json', 'name': name,
           'position': {'x': x, 'y': y, 'width': width, 'height': height, 'z': y + x, 'tabOrder': y + x},
           'visual': config}
    write(REPORT / f'definition/pages/{page}/visuals/{name}/visual.json', doc)


def textbox(page, name, text, rect, size='18pt'):
    visual(page, name, 'textbox', rect, objects={'general': [{'properties': {'paragraphs': [
        {'textRuns': [{'value': text, 'textStyle': {'fontFamily': 'Segoe UI', 'fontSize': size, 'color': '#19382E'}}]}
    ]}}]})


def main():
    tables = []
    definitions = {
        'ClimateMonthly': ('fact_climate_monthly.csv', {'site': 'string', 'year': 'int64', 'month': 'int64',
            'year_month': 'string', 'rainfall_mm': 'double', 'temperature_c': 'double', 'soil_moisture': 'double',
            'hours': 'int64', 'chirps_lat': 'double', 'chirps_lon': 'double', 'era5_lat': 'double', 'era5_lon': 'double'}),
        'Locations': ('dim_location.csv', {'id': 'string', 'name': 'string', 'zone': 'string', 'lat': 'double', 'lon': 'double'}),
        'Calendar': ('dim_month.csv', {'year_month': 'string', 'year': 'int64', 'month': 'int64', 'month_name': 'string', 'month_start': 'dateTime'}),
        'Infrastructure': ('fact_infrastructure.csv', {'site': 'string', 'healthcare': 'int64', 'education': 'int64', 'water': 'int64', 'road_km': 'double'}),
        'SourceAudit': ('source_audit.csv', {'dataset': 'string', 'year': 'int64', 'path': 'string', 'bytes': 'int64', 'sha256': 'string'}),
        'SeasonalDepartures': ('mart_monthly_anomalies.csv', {'site': 'string', 'year': 'int64', 'month': 'int64', 'year_month': 'string',
            'rainfall_mm': 'double', 'baseline_rainfall_mm': 'double', 'rainfall_anomaly_pct': 'double',
            'temperature_anomaly_c': 'double', 'soil_moisture_anomaly_pp': 'double', 'baseline_start_year': 'int64', 'baseline_end_year': 'int64'}),
        'WaterYears': ('mart_water_year.csv', {'site': 'string', 'water_year': 'int64', 'months_covered': 'int64',
            'is_complete': 'boolean', 'rainfall_mm': 'double', 'temperature_c': 'double', 'soil_moisture_pct': 'double'}),
        'LocationTrends': ('mart_site_trends.csv', {'site': 'string', 'observation_years': 'int64', 'first_year': 'int64', 'last_year': 'int64',
            'rainfall_slope_mm_per_year': 'double', 'temperature_slope_c_per_decade': 'double'}),
        'FootprintRainfall': ('mart_spatial_rainfall.csv', {'year': 'int64', 'month': 'int64', 'year_month': 'string',
            'area_weighted_rainfall_mm': 'double', 'valid_grid_cells': 'int64', 'approximate_coverage_km2': 'double'}),
    }
    m_types = {'string': 'type text', 'double': 'type number', 'int64': 'Int64.Type', 'dateTime': 'type date', 'boolean': 'type logical'}
    for table, (filename, cols) in definitions.items():
        columns = [{'name': key, 'dataType': value, 'sourceColumn': key, 'summarizeBy': 'none'} for key, value in cols.items()]
        for col in columns:
            if table == 'Locations' and col['name'] in ('lat', 'lon'):
                col['dataCategory'] = 'Latitude' if col['name'] == 'lat' else 'Longitude'
            if table == 'Calendar' and col['name'] == 'month_name':
                col['sortByColumn'] = 'month'
            if col['dataType'] == 'dateTime':
                col['formatString'] = 'yyyy-MM-dd'
        conversions = ', '.join('{"' + col + '", ' + m_types[typ] + '}' for col, typ in cols.items())
        expression = ['let',
                      '    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),',
                      f'    Gold = Source{{[Schema="gold", Item="{Path(filename).stem}"]}}[Data],',
                      '    Typed = Table.TransformColumnTypes(Gold, {' + conversions + '}, "en-US")', 'in', '    Typed']
        tables.append({'name': table, 'columns': columns,
                       'partitions': [{'name': table, 'mode': 'import', 'source': {'type': 'm', 'expression': expression}}]})
        (BI / 'queries').mkdir(parents=True, exist_ok=True)
        (BI / 'queries' / f'{table}.pq').write_text('\n'.join(expression) + '\n', encoding='utf-8')
    measures = {
        'Rainfall mm': ('AVERAGEX(SUMMARIZE(ClimateMonthly, ClimateMonthly[site], ClimateMonthly[year]), CALCULATE(SUM(ClimateMonthly[rainfall_mm])))', '#,0.0'),
        'Mean Temperature C': ('AVERAGEX(VALUES(Locations[id]), CALCULATE(DIVIDE(SUMX(ClimateMonthly, ClimateMonthly[temperature_c] * ClimateMonthly[hours]), SUM(ClimateMonthly[hours]))))', '0.0 °C'),
        'Surface Soil Moisture Percent': ('100 * AVERAGEX(VALUES(Locations[id]), CALCULATE(DIVIDE(SUMX(ClimateMonthly, ClimateMonthly[soil_moisture] * ClimateMonthly[hours]), SUM(ClimateMonthly[hours]))))', '0.0'),
        'Rainfall Reference 2015 mm': ("VAR SelectedMonths = VALUES('Calendar'[month]) RETURN CALCULATE([Rainfall mm], REMOVEFILTERS('Calendar'), 'Calendar'[year] = 2015, TREATAS(SelectedMonths, 'Calendar'[month]))", '#,0.0'),
        'Rainfall Change Percent': ('DIVIDE([Rainfall mm] - [Rainfall Reference 2015 mm], [Rainfall Reference 2015 mm])', '0.0%'),
        'Downloaded Climate GB': ('SUM(SourceAudit[bytes]) / 1000000000', '0.00'),
        'Climate Observation Months': ('COUNTROWS(ClimateMonthly)', '#,0'),
        'Mapped Water Objects': ('SUM(Infrastructure[water])', '#,0'),
        'Mapped Healthcare Objects': ('SUM(Infrastructure[healthcare])', '#,0'),
        'Mapped Education Objects': ('SUM(Infrastructure[education])', '#,0'),
        'Mapped Road km': ('SUM(Infrastructure[road_km])', '#,0.0'),
        'Rainfall Baseline Departure Percent': ('DIVIDE(SUM(SeasonalDepartures[rainfall_mm]) - SUM(SeasonalDepartures[baseline_rainfall_mm]), SUM(SeasonalDepartures[baseline_rainfall_mm]))', '0.0%'),
        'Complete Water Year Rainfall mm': ('AVERAGEX(FILTER(WaterYears, WaterYears[is_complete] = TRUE()), WaterYears[rainfall_mm])', '#,0.0'),
        'Rainfall Trend mm Per Year': ('AVERAGE(LocationTrends[rainfall_slope_mm_per_year])', '0.0'),
        'Temperature Trend C Per Decade': ('AVERAGE(LocationTrends[temperature_slope_c_per_decade])', '0.00'),
        'Footprint Monthly Rainfall mm': ('AVERAGE(FootprintRainfall[area_weighted_rainfall_mm])', '#,0.0'),
    }
    tables[0]['measures'] = [{'name': name, 'expression': expression, 'formatString': fmt}
                              for name, (expression, fmt) in measures.items()]
    (BI / 'measures.dax').write_text('// Generated from the native semantic model; create each as a measure.\n\n' +
                                   '\n\n'.join(f'{name} =\n{expression}' for name, (expression, _) in measures.items()) + '\n', encoding='utf-8')
    relationships = [
        {'name': 'ClimateLocation', 'fromTable': 'ClimateMonthly', 'fromColumn': 'site', 'toTable': 'Locations', 'toColumn': 'id'},
        {'name': 'ClimateCalendar', 'fromTable': 'ClimateMonthly', 'fromColumn': 'year_month', 'toTable': 'Calendar', 'toColumn': 'year_month'},
        {'name': 'InfrastructureLocation', 'fromTable': 'Infrastructure', 'fromColumn': 'site', 'toTable': 'Locations', 'toColumn': 'id'},
        {'name': 'DeparturesLocation', 'fromTable': 'SeasonalDepartures', 'fromColumn': 'site', 'toTable': 'Locations', 'toColumn': 'id'},
        {'name': 'DeparturesCalendar', 'fromTable': 'SeasonalDepartures', 'fromColumn': 'year_month', 'toTable': 'Calendar', 'toColumn': 'year_month'},
        {'name': 'WaterYearLocation', 'fromTable': 'WaterYears', 'fromColumn': 'site', 'toTable': 'Locations', 'toColumn': 'id'},
        {'name': 'TrendLocation', 'fromTable': 'LocationTrends', 'fromColumn': 'site', 'toTable': 'Locations', 'toColumn': 'id'},
        {'name': 'FootprintCalendar', 'fromTable': 'FootprintRainfall', 'fromColumn': 'year_month', 'toTable': 'Calendar', 'toColumn': 'year_month'},
    ]
    write(MODEL / 'definition.pbism', {'version': '1.0'})
    write(MODEL / 'model.bim', {'name': 'MoroccoClimate', 'compatibilityLevel': 1600,
          'model': {'culture': 'en-US', 'defaultPowerBIDataSourceVersion': 'powerBI_V3', 'tables': tables,
                     'relationships': relationships, 'expressions': [
                        {'name': 'PostgresServer', 'kind': 'm', 'expression': '"localhost:5432" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'},
                        {'name': 'PostgresDatabase', 'kind': 'm', 'expression': '"observatory" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'}]}})
    (BI / 'queries/PostgresServer.pq').write_text('"localhost:5432" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n', encoding='utf-8')
    (BI / 'queries/PostgresDatabase.pq').write_text('"observatory" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n', encoding='utf-8')
    write(BI / 'Morocco.pbip', {'version': '1.0', 'artifacts': [{'report': {'path': 'Observatory.Report'}}],
                              'settings': {'enableAutoRecovery': True}})
    write(REPORT / 'definition.pbir', {'$schema': SCHEMA + 'definitionProperties/2.0.0/schema.json',
          'version': '4.0', 'datasetReference': {'byPath': {'path': '../Observatory.SemanticModel'}}})
    write(REPORT / 'definition/report.json', {'$schema': SCHEMA + 'definition/report/1.0.0/schema.json',
          'layoutOptimization': 'None', 'themeCollection': {'customTheme': {'name': 'MoroccoTheme',
          'reportVersionAtImport': '5.59', 'type': 'RegisteredResources'}},
          'resourcePackages': [{'name': 'RegisteredResources', 'type': 'RegisteredResources',
          'items': [{'name': 'MoroccoTheme', 'path': 'MoroccoTheme.json', 'type': 'CustomTheme'}]}]})
    write(REPORT / 'StaticResources/RegisteredResources/MoroccoTheme.json', json.loads((BI / 'theme.json').read_text()))
    pages = [('overview', 'Climate overview'), ('locations', 'Location comparison'), ('insights', 'dbt water & climate insights'), ('sources', 'Sources & methodology')]
    write(REPORT / 'definition/pages/pages.json', {'$schema': SCHEMA + 'definition/pagesMetadata/1.0.0/schema.json',
                                                  'pageOrder': [p[0] for p in pages], 'activePageName': 'overview'})
    for page, title in pages:
        write(REPORT / f'definition/pages/{page}/page.json', {'$schema': SCHEMA + 'definition/page/1.0.0/schema.json',
              'name': page, 'displayName': title, 'displayOption': 'FitToPage', 'width': 1280, 'height': 800,
              'objects': {'background': [{'properties': {'color': {'solid': {'color': literal("'#F5F7F3'")}}, 'transparency': literal('0D')}}]}})
        textbox(page, 'heading', 'MOROCCO  /  ' + title.upper(), (28, 18, 830 if page != 'sources' else 1220, 45), '24pt')
        textbox(page, 'scope', 'CHIRPS + ERA5-Land + OpenStreetMap · 2015–2024 · 12 sample locations · nearby grid cells', (28, 64, 830 if page != 'sources' else 1220, 28), '10pt')
    p = lambda c: projection('ClimateMonthly', c, True)
    for i, name in enumerate(['Rainfall mm', 'Mean Temperature C', 'Surface Soil Moisture Percent', 'Downloaded Climate GB']):
        visual('overview', f'card{i}', 'card', (28 + i * 310, 105, 294, 105), {'Values': [p(name)]}, name)
    visual('overview', 'samplemap', 'azureMap', (28, 230, 590, 330),
           {'Category': [projection('Locations', 'name')], 'Y': [projection('Locations', 'lat')],
            'X': [projection('Locations', 'lon')], 'Size': [p('Rainfall mm')],
            'Tooltips': [p('Mean Temperature C'), p('Mapped Water Objects')]}, 'Rainfall at the sample locations',
           {'bubbleLayer': [{'properties': {'show': literal('true'), 'sizeByValue': literal('true')}}]})
    visual('overview', 'raintrend', 'lineChart', (640, 230, 610, 330), {'Category': [projection('Calendar', 'month_start')], 'Y': [p('Rainfall mm'), p('Rainfall Reference 2015 mm')]}, 'Monthly rainfall and 2015 comparison · mm')
    dropdown = {'data': [{'properties': {'mode': literal("'Dropdown'")}}]}
    visual('overview', 'year', 'slicer', (890, 14, 165, 80), {'Values': [projection('Calendar', 'year')]}, 'Year', dropdown)
    visual('overview', 'site', 'slicer', (1070, 14, 180, 80), {'Values': [projection('Locations', 'name')]}, 'Location', dropdown)
    visual('overview', 'moisture', 'lineChart', (28, 580, 790, 185), {'Category': [projection('Calendar', 'month_start')], 'Y': [p('Surface Soil Moisture Percent')]}, 'Surface soil moisture · % volume')
    visual('overview', 'watercontext', 'card', (842, 580, 195, 150), {'Values': [p('Mapped Water Objects')]}, 'Mapped water objects')
    textbox('overview', 'reference', 'Select a year for its annual rainfall. Without a year selection, rainfall is averaged across location-years. OSM is contemporary context.', (1050, 580, 200, 180), '11pt')
    visual('locations', 'rainranking', 'clusteredBarChart', (28, 115, 590, 320), {'Category': [projection('Locations', 'name')], 'Y': [p('Rainfall mm')]}, 'Rainfall by sample location · mm')
    visual('locations', 'year', 'slicer', (1050, 14, 200, 80), {'Values': [projection('Calendar', 'year')]}, 'Year', dropdown)
    visual('locations', 'tempranking', 'clusteredBarChart', (640, 115, 610, 320), {'Category': [projection('Locations', 'name')], 'Y': [p('Mean Temperature C')]}, 'Mean temperature · °C')
    visual('locations', 'contexttable', 'tableEx', (28, 465, 1220, 280), {'Values': [projection('Locations', 'name'), p('Rainfall Change Percent'), p('Surface Soil Moisture Percent'), p('Mapped Water Objects'), p('Mapped Healthcare Objects'), p('Mapped Education Objects'), p('Mapped Road km')]}, 'Climate and nearby infrastructure · mapped objects within 20 km')
    visual('insights', 'site', 'slicer', (1050, 14, 200, 80), {'Values': [projection('Locations', 'name')]}, 'Location', dropdown)
    visual('insights', 'departures', 'lineChart', (28, 115, 590, 250), {'Category': [projection('Calendar', 'month_start')], 'Y': [p('Rainfall Baseline Departure Percent')]}, 'Rainfall departures · short 2015–2019 reference')
    visual('insights', 'wateryears', 'clusteredColumnChart', (640, 115, 610, 250), {'Category': [projection('WaterYears', 'water_year')], 'Y': [p('Complete Water Year Rainfall mm')]}, 'Complete October–September water years · mean sampled rainfall')
    visual('insights', 'spatial', 'lineChart', (28, 390, 590, 250), {'Category': [projection('Calendar', 'month_start')], 'Y': [p('Footprint Monthly Rainfall mm')]}, 'Area-weighted rainfall · full extract footprint, independent of location filter')
    visual('insights', 'trends', 'tableEx', (640, 390, 610, 250), {'Values': [projection('Locations', 'name'), p('Rainfall Trend mm Per Year'), p('Temperature Trend C Per Decade')]}, 'Exploratory slopes · all ten complete calendar years')
    textbox('insights', 'limits', 'dbt Gold marts: a five-year reference is not a 30-year normal or an official drought index. Incomplete water years are excluded from totals. Slopes are descriptive, without significance tests or forecasts. The footprint mean is not an official national statistic.', (28, 665, 1220, 105), '12pt')
    visual('sources', 'auditsize', 'card', (28, 110, 294, 100), {'Values': [p('Downloaded Climate GB')]}, 'Climate source archives · GB')
    visual('sources', 'auditrows', 'card', (340, 110, 294, 100), {'Values': [p('Climate Observation Months')]}, 'Joined location-months')
    textbox('sources', 'limitations', 'Source archive size is global, not Morocco-only. CHIRPS is rainfall estimation; ERA5-Land is reanalysis. Sample sites are not national averages. OSM objects do not establish service completeness or causal climate impacts.', (660, 110, 590, 105), '12pt')
    visual('sources', 'auditfiles', 'tableEx', (28, 240, 1220, 385), {'Values': [projection('SourceAudit', c) for c in ('dataset', 'year', 'bytes', 'path', 'sha256')]}, 'Downloaded climate source provenance')
    textbox('sources', 'attribution', 'Sources: UCSB CHC CHIRPS v3 · Copernicus ERA5-Land via Open-Meteo (CC BY 4.0; free API for non-commercial use) · © OpenStreetMap contributors / Geofabrik (ODbL). Reference rainfall year: 2015; this is a comparison, not a long-term climate normal.', (28, 650, 1220, 115), '12pt')
    print('Native Power BI project written to ' + str(BI / 'Morocco.pbip'))


if __name__ == '__main__':
    main()
