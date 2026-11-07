from __future__ import annotations

import calendar
import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import charts
from dashboard.data import (
    DEFAULT_DATA_ROOT, MEASURES, aggregate_sites, assets_for_sites, cohort_summary,
    comparison_table, csv_bytes, file_signature, filter_climate, load_bundle, monthly_series,
)

PAGES = ['Overview', 'Indicators', 'Climate explorer', 'Compare locations', 'Water & climate insights', 'Infrastructure', 'Data & methodology']
TITLES = {
    'Indicators': ('Read the signals in the data.', 'Explore supported indicators, their formulas, and complete-year profiles.'),
    'Overview': ('Morocco, in perspective.', 'Follow the rain. Understand the land. Explore the connections.'),
    'Climate explorer': ('The shape of a changing climate.', 'Explore monthly observations, seasonal patterns, and departures from your reference year.'),
    'Compare locations': ('Every location tells a different story.', 'Compare sampled climate conditions on a shared scale.'),
    'Infrastructure': ('Put the climate in context.', 'Explore mapped services and water assets around the sample locations.'),
    'Water & climate insights': ('Turn observations into understanding.', 'Explore tested dbt marts: seasonal departures, water years, spatial coverage, and exploratory trends.'),
    'Data & methodology': ('Trace every insight to its source.', 'Understand coverage, quality, provenance, and the limits of each comparison.'),
}


@st.cache_data(show_spinner=False, max_entries=4)
def cached_bundle(root, signature):
    return load_bundle(Path(root))


@st.cache_data(ttl=30, show_spinner='Loading the committed data snapshot…')
def remote_root():
    from dashboard.backend import minio_snapshot
    return minio_snapshot()


@st.cache_data(show_spinner=False, max_entries=4)
def geometry(root, name, signature):
    path = Path(root) / 'gold' / name
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else None


@st.cache_data(ttl=30, show_spinner='Reading the committed PostgreSQL Gold publication?')
def postgres_bundle():
    from dashboard.postgres_backend import load_postgres_bundle
    return load_postgres_bundle()


def bundle_geometry(bundle, name):
    if bundle.get('assets') is not None:
        return bundle['assets'].get(name)
    return geometry(bundle['root'], name, file_signature(Path(bundle['root'])))


def chart(fig, key):
    st.plotly_chart(fig, width='stretch', theme=None, key=key,
                    config={'displaylogo': False, 'toImageButtonOptions': {'format': 'png', 'scale': 2}})


def source_footer(bundle):
    st.divider()
    cols = st.columns(3)
    for col, source in zip(cols, bundle['snapshot']['sources']):
        with col:
            st.markdown(f"**{source['name']}**")
            st.caption(source['role'])
            st.link_button('Source documentation ↗', source['url'])


def metrics(table, selected, ref_year):
    summary = cohort_summary(table)
    a, b, c, d = st.columns(4)
    change = summary['rain_change_pct']
    a.metric('Mean period rainfall', f"{summary['rainfall_mm']:,.1f} mm",
             f'{change:+.1f}% vs {ref_year}' if change is not None else None, delta_color='off', border=True,
             chart_data=monthly_series(selected, 'rainfall_mm').tolist(), chart_type='area',
             help='Mean of period rainfall totals across selected location-years. Not a national total.')
    b.metric('Mean temperature', f"{summary['temperature_c']:.1f} °C", f"{summary['temp_delta_c']:+.1f} °C vs {ref_year}",
             delta_color='off', border=True, chart_data=monthly_series(selected, 'temperature_c').tolist(),
             help='Hour-weighted within each location, then an equal-weight mean across selected locations.')
    c.metric('Surface soil moisture', f"{summary['soil_pct']:.1f}% vol.", f"{summary['soil_delta_pp']:+.1f} pp vs {ref_year}",
             delta_color='off', border=True, chart_data=monthly_series(selected, 'soil_pct').tolist(),
             help='Volumetric water content in the top 0–7 cm, expressed as a percentage. Change is in percentage points.')
    count = int(table[['healthcare', 'education', 'water']].sum().sum())
    d.metric('Mapped nearby services', f'{count:,}', 'Contemporary OSM snapshot', delta_color='off', border=True,
             help='Sum of mapped healthcare, education and water object counts within 20 km of each selected site. Not service capacity; neighborhoods can overlap.')
    st.caption(f'{len(table)} sampled locations · {len(selected):,} location-months · matching-month comparison with {ref_year}. '
               'Nearby climate grid cells, not complete cities or national averages.')


def show_map(bundle, table, metric, unit, online, assets=None, key='map'):
    boundary = bundle_geometry(bundle, 'footprint.geojson')
    deck = charts.climate_map(table, metric, unit, assets=assets, footprint=boundary, online=online)
    st.pydeck_chart(deck, width='stretch', height=420, key=key)
    st.caption(f"Climate dots: {table[metric].min():.1f}–{table[metric].max():.1f} {unit}; pale gold → deep green. "
               'Outline: Geofabrik extract footprint. © OpenStreetMap contributors. CARTO basemap when enabled.')


def comparison_dataframe(table):
    columns = {'name': 'Location', 'zone': 'Climate context', 'rainfall_mm': 'Rainfall (mm)',
               'rain_change_pct': 'Rainfall change (%)', 'temperature_c': 'Temperature (°C)',
               'temp_delta_c': 'Temperature change (°C)', 'soil_pct': 'Soil moisture (% vol.)',
               'soil_delta_pp': 'Soil moisture change (pp)', 'water': 'Mapped water objects',
               'healthcare': 'Mapped healthcare objects', 'education': 'Mapped education objects', 'road_km': 'Mapped road km'}
    return table[list(columns)].rename(columns=columns).round(2)


def overview(bundle, table, selected, reference, ref_year, online):
    metric_name = st.radio('Map and chart measure', list(MEASURES), horizontal=True, key='overview_measure')
    metric, unit, _ = MEASURES[metric_name]
    left, right = st.columns([1.2, 1])
    with left, st.container(border=True):
        st.subheader('A landscape of differences')
        show_map(bundle, table, metric, unit, online, key='overview_map')
    with right, st.container(border=True):
        st.subheader('The seasonal picture')
        chart(charts.seasonality(selected, reference, metric, unit, ref_year), 'overview_season')
        wettest = table.loc[table.rainfall_mm.idxmax()]
        st.info(f"{wettest['name']} has the largest sampled rainfall total in this view: {wettest['rainfall_mm']:,.1f} mm per selected period-year.")
    with st.container(border=True):
        st.subheader('Locations at a glance')
        st.dataframe(comparison_dataframe(table), hide_index=True, width='stretch')
        st.download_button('Download comparison table', csv_bytes(comparison_dataframe(table)),
                           'morocco_location_comparison.csv', 'text/csv', key='overview_export')
    source_footer(bundle)


def explorer(bundle, table, selected, reference, ref_year):
    label = st.selectbox('Climate variable', list(MEASURES), key='explorer_measure')
    metric, unit, _ = MEASURES[label]
    individual = st.toggle('Show individual location traces', value=False, key='individual_traces')
    with st.container(border=True):
        st.subheader('Monthly observations')
        chart(charts.timeline(selected, reference, metric, unit, ref_year, individual), 'history_lines')
        st.caption('Unselected months appear as gaps. Hover, zoom, pan, or export charts using the chart toolbar.')
    a, b = st.columns(2)
    with a, st.container(border=True):
        st.subheader('Year × month')
        chart(charts.history_heatmap(selected, reference, metric, unit), 'history_heatmap')
    with b, st.container(border=True):
        st.subheader(f'Departures from {ref_year}')
        chart(charts.history_heatmap(selected, reference, metric, unit, anomalies=True), 'anomaly_heatmap')
        st.caption('Rainfall: percentage change; temperature: °C difference; moisture: percentage points. '
                   'Zero reference rainfall yields an empty percentage cell. This is not an official drought classification.')
    st.subheader('Observed monthly distribution')
    fig = px.box(selected, x='name', y=metric, color='zone', points=False,
                 labels={'name': 'Location', metric: unit, 'zone': 'Context'})
    chart(charts.style(fig, height=380), 'monthly_distribution')
    with st.expander('Inspect the exact grid cells and observations'):
        st.dataframe(selected.drop(columns='date'), hide_index=True, width='stretch')
        st.caption('CHIRPS and ERA5 coordinates can differ from the requested city coordinates because their grids have different resolutions.')


def compare(bundle, table, selected):
    label = st.selectbox('Ranking variable', list(MEASURES), key='ranking_measure')
    metric, unit, _ = MEASURES[label]
    a, b = st.columns([1, 1.1])
    with a, st.container(border=True):
        st.subheader('One measure, one shared scale')
        chart(charts.ranking(table, metric, unit), 'ranking')
    with b, st.container(border=True):
        st.subheader('Heat, rain, and mapped water context')
        fig = px.scatter(table, x='temperature_c', y='rainfall_mm', size='water', color='zone',
                         hover_name='name', size_max=44,
                         hover_data={'water': True, 'soil_pct': ':.1f', 'temperature_c': ':.1f', 'rainfall_mm': ':.1f'},
                         labels={'temperature_c': 'Mean temperature (°C)', 'rainfall_mm': 'Mean period rainfall (mm)',
                                 'water': 'Mapped water objects', 'zone': 'Context', 'soil_pct': 'Soil moisture (% vol.)'})
        chart(charts.style(fig, height=max(380, len(table) * 30 + 70)), 'climate_scatter')
        st.caption('Bubble size is mapped object count, not water supply or storage capacity. Co-occurrence does not demonstrate causation.')
    st.dataframe(comparison_dataframe(table), hide_index=True, width='stretch')
    st.download_button('Export comparison CSV', csv_bytes(comparison_dataframe(table)), 'morocco_comparison.csv', 'text/csv')


def infrastructure(bundle, table, online):
    geojson = bundle_geometry(bundle, 'osm.geojson')
    if geojson is None:
        st.warning('The mapped OSM objects are missing. Run the processing pipeline to generate osm.geojson.')
        return
    options = ['all'] + table['site'].tolist()
    lookup = table.set_index('site')['name'].to_dict()
    a, b = st.columns(2)
    focus = a.selectbox('Map focus', options, format_func=lambda s: 'All selected locations' if s == 'all' else lookup[s], key='asset_focus')
    category = b.selectbox('Show mapped objects', ['All services', 'Water assets', 'Healthcare', 'Education'], key='asset_category')
    categories = {'All services': None, 'Water assets': ['water'], 'Healthcare': ['healthcare'], 'Education': ['education']}[category]
    view = table if focus == 'all' else table[table.site.eq(focus)]
    features = assets_for_sites(geojson, view.site, categories)
    all_features = assets_for_sites(geojson, view.site)
    cols = st.columns(3)
    for col, key, name in zip(cols, ['water', 'healthcare', 'education'], ['Water objects', 'Healthcare objects', 'Education objects']):
        col.metric(name, f"{sum(f['properties']['category'] == key for f in all_features):,}", border=True)
    st.caption('Unique mapped objects around the map focus, across all categories. Context is a contemporary OSM snapshot, not historical infrastructure.')
    show_map(bundle, view, 'rainfall_mm', 'mm', online, assets=features, key='asset_map')
    st.caption(f'{len(features):,} objects in the selected category view. Blue: water · rust: healthcare · gold: education. Circles mark 20 km neighborhoods.')
    st.download_button('Download selected OSM objects (GeoJSON)', json.dumps({'type': 'FeatureCollection', 'features': features}, ensure_ascii=False),
                       'morocco_mapped_objects.geojson', 'application/geo+json')
    st.subheader('Services across the selected locations')
    melted = table[['name', 'water', 'healthcare', 'education']].melt(id_vars='name', var_name='Category', value_name='Mapped objects')
    chart(charts.style(px.bar(melted, x='name', y='Mapped objects', color='Category', barmode='group',
                              color_discrete_map={'water': charts.BLUE, 'healthcare': charts.COPPER, 'education': '#A8A267'},
                              labels={'name': 'Location'}), height=380), 'asset_counts')
    st.caption('Counts are mapped objects. Mapping completeness and the meaning of a service object vary by location.')


def quality(bundle):
    meta = bundle['snapshot']['meta']
    audit = bundle['audit']
    volume = int(audit['bytes'].sum()) if not audit.empty else bundle['metadata']['verification'].get('climate_source_bytes', 0)
    a, b, c, d = st.columns(4)
    a.metric('Recorded climate sources', f'{volume / 1e9:.2f} GB', border=True)
    b.metric('Rainfall grid records', f"{meta['chirps_grid_records']:,}", border=True)
    c.metric('ERA5-Land hourly records', f"{meta['era5_hourly_records']:,}", border=True)
    d.metric('Joined location-months', f"{len(bundle['fact']):,}", border=True)
    st.info('The large CHIRPS source archives are global. Regional processing clips them to the Geofabrik extract footprint; '
            'the location dashboard samples twelve points. Source archive size is not the size of a Morocco-only subset.')
    source_footer(bundle)
    st.subheader('Quality and coverage')
    checks = pd.DataFrame([
        {'Check': 'Complete location/calendar coverage', 'Result': 'PASS', 'Evidence': f"{len(bundle['locations'])} locations × {len(bundle['months'])} months"},
        {'Check': 'Unique climate keys and dimension relationships', 'Result': 'PASS', 'Evidence': 'Validated when the tables load'},
        {'Check': 'Nonnegative rainfall; moisture in [0, 1]; positive hours', 'Result': 'PASS', 'Evidence': 'Validated against all Gold observations'},
        {'Check': 'Downloaded climate volume above 1 GiB', 'Result': 'PASS' if volume > 1024**3 else 'NOT MET', 'Evidence': f'{volume:,} source bytes in the acquisition records'},
    ])
    st.dataframe(checks, hide_index=True, width='stretch')
    st.caption('Volume and checksums are recorded acquisition results. Opening the dashboard does not rescan or rehash the raw archives.')
    result = bundle['metadata'].get('dbt', {})
    if result:
        statuses = pd.DataFrame([{'Node': row['unique_id'], 'Status': row['status'], 'Seconds': row.get('execution_time')} for row in result.get('results', [])])
        st.subheader('dbt transformation evidence')
        st.caption(f"Invocation {result.get('metadata', {}).get('invocation_id', 'unknown')} · engine: {meta.get('gold_transformation_engine', 'not recorded')}")
        st.dataframe(statuses, hide_index=True, width='stretch')
    if not audit.empty:
        st.subheader('Climate source inventory')
        chart(charts.style(px.bar(audit.assign(MB=audit['bytes'] / 1e6), x='year', y='MB', color='dataset', barmode='group',
                                  labels={'year': 'Source year', 'MB': 'Source file size (MB)', 'dataset': 'Dataset'})), 'source_volumes')
        st.dataframe(audit, hide_index=True, width='stretch')
        st.download_button('Download source inventory', csv_bytes(audit), 'morocco_climate_sources.csv', 'text/csv')
    with st.expander('Calculation methods and interpretation', expanded=True):
        st.markdown('''
**Rainfall:** sum the selected months within each location-year, then average those period totals across selected years and locations. It is not the sum of rain falling across Morocco.

**Temperature and soil moisture:** weight monthly means by their hourly record counts within each location, then give each selected location equal weight. Moisture is volumetric water content at 0–7 cm; percentage-point changes differ from percentage changes.

**Comparisons:** use the selected reference year and the same months and locations. Rainfall change is `(selected mean − reference mean) / reference mean × 100`. If reference rainfall is zero, percentage change is undefined and shown as empty. This single-year comparison is not a 30-year climate normal or an official drought classification.

**Mapping:** climate markers use requested sample locations. The exact CHIRPS and ERA5 cell coordinates are in the exported observations. OSM assets are mapped objects within 20 km. Counts are not service capacity, population exposure, or evidence of a causal climate impact.

**Time scope:** the climate observations end in 2024. Infrastructure represents the contemporary extract at processing time; it is not reconstructed for historical climate years. Basemap tiles are an optional online dependency.
''')
    st.download_button('Download dataset metadata', json.dumps(bundle['snapshot']['meta'], indent=2), 'morocco_snapshot_metadata.json', 'application/json')


def insights(bundle, selected, sites, years, months):
    marts = bundle['marts']
    if len(marts) != 4:
        st.info('Build the dbt Gold marts to enable these analyses: python observatory/warehouse.py local-all')
        return
    tabs = st.tabs(['Seasonal departures', 'Water years', 'Full rainfall footprint', 'Location trends'])
    with tabs[0]:
        data = marts['mart_monthly_anomalies']
        view = data[data.site.isin(sites) & data.year.between(*years) & data.month.isin(months)].copy()
        view['date'] = pd.to_datetime(dict(year=view.year, month=view.month, day=1))
        view = view.merge(bundle['locations'][['id', 'name']], left_on='site', right_on='id')
        st.caption('Reference period: 2015–2019, comparing the same calendar month at each sample. This five-year reference is not a 30-year normal or official drought index. Empty percentages indicate zero reference rainfall.')
        for column, label in [('rainfall_anomaly_pct', 'Rainfall departure (%)'), ('temperature_anomaly_c', 'Temperature departure (°C)'), ('soil_moisture_anomaly_pp', 'Soil moisture departure (pp)')]:
            fig = px.scatter(view, x='date', y=column, color='name', labels={column: label, 'name': 'Location', 'date': 'Observation month'})
            fig.add_hline(y=0, line_dash='dot', line_color='#8b968e')
            chart(charts.style(fig), 'mart_' + column)
        st.download_button('Export seasonal departures', csv_bytes(view), 'morocco_seasonal_departures.csv', 'text/csv')
    with tabs[1]:
        st.caption('Each water year runs from October through September and is labeled by its ending year. This view uses all months and the selected ending-year range; the Months filter does not change water-year totals.')
        water = marts['mart_water_year']
        view = water[water.site.isin(sites) & water.water_year.between(*years)]
        complete = view[view.is_complete.eq(True)].merge(bundle['locations'][['id', 'name']], left_on='site', right_on='id')
        if complete.empty:
            st.info('No complete water years in this selection. Try 2016–2024.')
        else:
            chart(charts.style(px.line(complete.sort_values('water_year'), x='water_year', y='rainfall_mm', color='name', markers=True,
                                      labels={'water_year': 'Water year ending', 'rainfall_mm': 'Water-year rainfall (mm)', 'name': 'Location'})), 'water_years')
        st.dataframe(view, hide_index=True, width='stretch')
        st.caption('Incomplete boundary years are marked is_complete=false and excluded from the chart.')
    with tabs[2]:
        spatial = marts['mart_spatial_rainfall']
        view = spatial[spatial.year.between(*years) & spatial.month.isin(months)].copy()
        view['date'] = pd.to_datetime(dict(year=view.year, month=view.month, day=1))
        st.caption('All valid rainfall cells inside the Geofabrik extract footprint, weighted by grid-cell area. Uses the Years and Months filters and the full footprint independently of selected locations. This is not an official national statistic.')
        a, b = st.columns(2)
        a.metric('Valid cells per month', f'{int(view.valid_grid_cells.min()):,}')
        b.metric('Approximate grid coverage', f'{view.approximate_coverage_km2.mean():,.0f} km²')
        chart(charts.style(px.bar(view, x='date', y='area_weighted_rainfall_mm', labels={'date': 'Observation month', 'area_weighted_rainfall_mm': 'Area-weighted rainfall (mm)'})), 'spatial_rainfall')
        st.download_button('Export footprint rainfall', csv_bytes(view), 'morocco_footprint_rainfall.csv', 'text/csv')
    with tabs[3]:
        trends = marts['mart_site_trends']
        view = trends[trends.site.isin(sites)].merge(bundle['locations'][['id', 'name']], left_on='site', right_on='id')
        st.caption('OLS slopes use all ten complete calendar years (2015–2024) at each selected location; Years and Months filters do not refit the trend. Ten years are short for climate trend estimation. These are descriptive slopes without significance tests or forecasts.')
        a, b = st.columns(2)
        with a:
            chart(charts.style(px.bar(view.sort_values('rainfall_slope_mm_per_year'), x='rainfall_slope_mm_per_year', y='name', orientation='h', labels={'rainfall_slope_mm_per_year': 'Rainfall slope (mm/year)', 'name': 'Location'})), 'rain_slopes')
        with b:
            chart(charts.style(px.bar(view.sort_values('temperature_slope_c_per_decade'), x='temperature_slope_c_per_decade', y='name', orientation='h', labels={'temperature_slope_c_per_decade': 'Temperature slope (°C/decade)', 'name': 'Location'})), 'temperature_slopes')
        st.dataframe(view, hide_index=True, width='stretch')



def indicators_page(bundle, sites, years):
    from dashboard.indicators import annual_profiles, indicator_catalog
    profiles_tab, catalog_tab = st.tabs(['Complete-year profiles', 'Indicator catalog'])
    with profiles_tab:
        st.caption('Profiles use complete calendar years in the Years range and selected Locations. The Months and single Reference year controls do not change these profiles. Monthly condition frequencies use the fixed 2015–2019 same-calendar-month reference.')
        fact = bundle['fact']
        fact = fact[fact.site.isin(sites) & fact.year.between(*years)]
        anomalies = bundle['marts'].get('mart_monthly_anomalies')
        if anomalies is None:
            st.info('The monthly anomaly mart is required for complete indicator profiles.')
        else:
            view = annual_profiles(fact, anomalies).merge(bundle['locations'][['id', 'name']], left_on='site', right_on='id')
            if view.empty:
                st.info('No complete calendar years are available in this selection.')
            else:
                a, b, c = st.columns(3)
                a.metric('Mean full-year rainfall', f'{view.mean_annual_rainfall_mm.mean():,.1f} mm', border=True,
                         help='Equal-weight mean across the selected location profiles; not a national rainfall total.')
                variability = view.rainfall_cv_pct.mean()
                b.metric('Annual rainfall variability', f'{variability:.1f}% CV' if pd.notna(variability) else 'Needs ≥2 years', border=True)
                c.metric('Warmer and drier months', f'{view.warm_dry_months_pct.mean():.1f}%', border=True,
                         help='Mean location share of eligible months with below-reference rainfall and above-reference temperature.')
                st.caption('CV uses the sample standard deviation of annual totals. A five-year reference and ten-year history support exploration; these are not official drought classifications.')
                left, right = st.columns(2)
                with left:
                    chart(charts.style(px.bar(view.sort_values('wettest_three_month_share_pct'), x='wettest_three_month_share_pct', y='name', orientation='h',
                        labels={'wettest_three_month_share_pct': 'Rain in each year’s wettest three months (%)', 'name': 'Location'})), 'indicator_concentration')
                with right:
                    chart(charts.style(px.bar(view.sort_values('warm_dry_months_pct'), x='warm_dry_months_pct', y='name', orientation='h',
                        labels={'warm_dry_months_pct': 'Warmer and drier months (%)', 'name': 'Location'})), 'indicator_warm_dry')
                export = view.drop(columns=['id']).rename(columns={'name': 'Location'})
                st.dataframe(export.round(2), hide_index=True, width='stretch')
                st.download_button('Export indicator profiles', csv_bytes(export), 'morocco_indicator_profiles.csv', 'text/csv')
    with catalog_tab:
        catalog = indicator_catalog()
        family = st.selectbox('Indicator family', ['All families', *catalog.Family.unique()], key='indicator_family')
        st.caption(f'{len(catalog)} supported indicators. Each definition states its unit, source table, formula and limits.')
        st.dataframe(catalog if family == 'All families' else catalog[catalog.Family.eq(family)], hide_index=True, width='stretch')
        st.download_button('Export indicator catalog', csv_bytes(catalog), 'morocco_indicator_catalog.csv', 'text/csv')
        st.info('Daily heatwaves, dry-spell duration, SPI/SPEI, reservoir storage, groundwater, irrigation demand and population exposure require additional data or a longer reference period. They are not computed from these monthly Gold tables.')


def main(*, data_root=None, backend=None):
    st.set_page_config(page_title='AtlasPulse · Morocco Climate Observatory', page_icon='✳️', layout='wide')
    st.markdown('''<style>
    .block-container {max-width:1500px;padding-top:4.75rem;padding-bottom:2rem}
    h1,h2,h3 {letter-spacing:-.025em} h1 {font-size:2.35rem!important;font-weight:700!important}
    h3 {font-size:1.12rem!important} [data-testid="stMetric"] {background:#FFF;border-radius:12px;padding:18px!important}
    [data-testid="stMetricValue"] {font-size:1.9rem} [data-testid="stMetricLabel"] {font-size:.8rem}
    [data-testid="stSidebar"] .block-container {padding-top:2rem}
    .observatory-title {font-size:clamp(1.85rem,3.5vw,3.25rem)!important;line-height:1.12!important;font-weight:800!important;letter-spacing:-.035em!important;color:#123D32;margin:0 0 .75rem;padding:0 0 .65rem;border-bottom:2px solid #B8D4A0;overflow-wrap:break-word}
    .brand {font-size:1.8rem;letter-spacing:-.04em;font-weight:700;color:#F0F5EA}.brand small {display:block;font-size:.57rem;letter-spacing:.21em;color:#B8D0AE}
    .section-note {color:#78876C;font-size:.88rem;line-height:1.7} [data-testid="stVerticalBlockBorderWrapper"] {border-radius:12px}
    </style>''', unsafe_allow_html=True)
    root = Path(data_root if data_root is not None else os.environ.get('OBSERVATORY_DATA_ROOT', str(DEFAULT_DATA_ROOT))).resolve()
    backend = backend or os.environ.get('OBSERVATORY_DATA_BACKEND', 'postgres')
    try:
        if backend == 'postgres':
            bundle = postgres_bundle()
        else:
            if backend == 'minio':
                root = Path(remote_root())
            bundle = cached_bundle(str(root), file_signature(root))
    except Exception as exc:
        # Never show connector exception text: endpoint errors can contain credentials.
        st.error(f'Gold data is unavailable or failed validation ({type(exc).__name__}). Build and publish a tested snapshot, then refresh.')
        if backend == 'local':
            st.info('Include the published Gold files with this application, then reload.')
        else:
            st.code('python observatory/serving.py publish-local')
        st.stop()
    fact = bundle['fact']
    available_years = sorted(int(y) for y in fact.year.unique())
    ids = bundle['locations']['id'].tolist()
    names = bundle['locations'].set_index('id')['name'].to_dict()

    def reset():
        st.session_state['locations'] = ids
        st.session_state['years'] = (available_years[-1], available_years[-1])
        st.session_state['months'] = list(range(1, 13))
        st.session_state['reference'] = available_years[0]

    with st.sidebar:
        st.markdown('<div class="brand">✳ AtlasPulse<small>MOROCCO OBSERVATORY</small></div>', unsafe_allow_html=True)
        st.caption('Climate · geography · connected context')
        st.divider()
        page = st.radio('Workspace', PAGES, key='page')
        st.divider()
        sites = st.multiselect('Locations', ids, default=ids, format_func=lambda value: names[value], key='locations')
        years = st.slider('Years', min_value=available_years[0], max_value=available_years[-1],
                          value=(available_years[-1], available_years[-1]), key='years')
        months = st.multiselect('Months', list(range(1, 13)), default=list(range(1, 13)),
                                format_func=lambda month: calendar.month_abbr[month], key='months')
        ref_year = st.selectbox('Reference year', available_years, key='reference',
                                help='Compare the same months and locations against this year. A reference year is not a long-term climate normal.')
        online = st.toggle('Online basemap', value=True, help='Turn off to use a plain map background without tile requests.', key='online_map')
        st.button('Reset filters', on_click=reset, width='stretch')
        st.divider()
        st.caption(f"OBSERVATIONS  {available_years[0]}–{available_years[-1]}\n\n12 sampled locations · no station readings")
        st.caption('Power BI companion · same published Gold data')
    selected = filter_climate(fact, sites, years, months)
    st.markdown('<h1 class="observatory-title">MOROCCO / CLIMATE &amp; WATER INTELLIGENCE</h1>', unsafe_allow_html=True)
    st.header(TITLES[page][0])
    st.markdown(f'<div class="section-note">{TITLES[page][1]}</div>', unsafe_allow_html=True)
    a, b = st.columns([4, 1])
    a.caption(f"{years[0]}–{years[1]} · {len(sites)} locations · {len(months)} selected months · reference {ref_year} · "
              f"snapshot built {bundle['snapshot']['meta']['built_at'][:10]}")
    b.download_button('↓ Export filtered CSV', csv_bytes(selected.drop(columns='date')), 'morocco_filtered_climate.csv',
                      'text/csv', disabled=selected.empty, key='filtered_export', width='stretch')
    if page == 'Data & methodology':
        quality(bundle)
        return
    if page == 'Indicators':
        if not sites:
            st.info('Choose at least one location in the sidebar. Reset filters restores the default view.')
        else:
            indicators_page(bundle, sites, years)
            source_footer(bundle)
        return
    if selected.empty:
        st.info('Choose at least one location and one month in the sidebar. Reset filters restores the default view.')
        return
    reference = fact[fact.site.isin(sites) & fact.year.eq(ref_year) & fact.month.isin(months)]
    table = comparison_table(bundle, selected, ref_year, months)
    metrics(table, selected, ref_year)
    st.divider()
    if page == 'Overview':
        overview(bundle, table, selected, reference, ref_year, online)
    elif page == 'Climate explorer':
        explorer(bundle, table, selected, reference, ref_year)
    elif page == 'Compare locations':
        compare(bundle, table, selected)
    elif page == 'Infrastructure':
        infrastructure(bundle, table, online)
    elif page == 'Water & climate insights':
        insights(bundle, selected, sites, years, months)
    st.caption('AtlasPulse · CHIRPS rainfall estimates / ERA5-Land reanalysis / © OpenStreetMap contributors. Exploratory analysis, not an official drought assessment.')
