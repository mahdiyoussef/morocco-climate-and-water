"""Interactive chart and map builders; all values come from the published tables."""
from __future__ import annotations

import calendar
import html
import math

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk

from dashboard.data import monthly_series

GREEN = '#19745B'
COPPER = '#BC8052'
BLUE = '#518AA3'
COLORS = [GREEN, COPPER, BLUE, '#A3AC65', '#866D99', '#4C9B91', '#C8A164', '#748A70', '#996F65', '#5988A1', '#8C9772', '#BA9C89']


def style(fig, title=None, height=350):
    fig.update_layout(template='plotly_white', height=height, title=title,
                      font=dict(family='Segoe UI, sans-serif', color='#19382E', size=12),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      margin=dict(l=15, r=15, t=40 if title else 18, b=25),
                      legend=dict(orientation='h', y=1.14, x=0), hovermode='x unified',
                      colorway=COLORS)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor='#E6EBDD', zeroline=False)
    return fig


def timeline(frame, reference, metric, unit, reference_year, individual=False):
    fig = go.Figure()
    dates = pd.date_range(f'{frame.year.min()}-01-01', f'{frame.year.max()}-12-01', freq='MS')
    if individual:
        for name, group in frame.groupby('name', sort=True):
            series = monthly_series(group, metric).reindex(dates)
            fig.add_scatter(x=dates, y=series, mode='lines+markers', name=name, connectgaps=False,
                            marker=dict(size=4), line=dict(width=2))
    else:
        series = monthly_series(frame, metric).reindex(dates)
        fig.add_scatter(x=dates, y=series, mode='lines+markers', name='Selected locations',
                        connectgaps=False, line=dict(color=GREEN, width=3), marker=dict(size=5))
        baseline = monthly_series(reference, metric)
        by_month = {date.month: value for date, value in baseline.items()}
        values = [by_month.get(d.month, np.nan) if d.month in set(frame.month) else np.nan for d in dates]
        fig.add_scatter(x=dates, y=values, mode='lines+markers', name=f'{reference_year} comparison',
                        connectgaps=False, line=dict(color=COPPER, dash='dot', width=2), marker=dict(size=4))
    fig.update_yaxes(title=unit)
    fig.update_traces(hovertemplate='%{x|%b %Y}<br>%{y:.2f} ' + unit + '<extra>%{fullData.name}</extra>')
    return style(fig, height=370)


def seasonality(frame, reference, metric, unit, reference_year):
    def values(data):
        out = {}
        for month, group in data.groupby('month'):
            out[month] = group[metric].mean() if metric == 'rainfall_mm' else np.average(group[metric], weights=group['hours'])
        return [out.get(m, np.nan) for m in range(1, 13)]
    fig = go.Figure()
    names = [calendar.month_abbr[m] for m in range(1, 13)]
    fig.add_bar(x=names, y=values(frame), name='Selected years', marker_color=GREEN)
    fig.add_scatter(x=names, y=values(reference), name=f'{reference_year} comparison', mode='lines+markers',
                    connectgaps=False, line=dict(color=COPPER, dash='dot', width=2))
    fig.update_yaxes(title=unit)
    return style(fig)


def history_heatmap(frame, reference, metric, unit, anomalies=False):
    rows = []
    for (year, month), group in frame.groupby(['year', 'month']):
        value = group[metric].mean() if metric == 'rainfall_mm' else np.average(group[metric], weights=group['hours'])
        ref = reference[reference.month.eq(month)]
        if anomalies and not ref.empty:
            baseline = ref[metric].mean() if metric == 'rainfall_mm' else np.average(ref[metric], weights=ref['hours'])
            value = (100 * (value - baseline) / baseline if baseline > 0 else np.nan) if metric == 'rainfall_mm' else value - baseline
        rows.append({'year': year, 'month': month, 'value': value})
    pivot = pd.DataFrame(rows).pivot(index='year', columns='month', values='value').reindex(columns=range(1, 13))
    label = ('% change' if metric == 'rainfall_mm' else ('percentage points' if metric == 'soil_pct' else '°C difference')) if anomalies else unit
    if anomalies:
        scale = [BLUE, '#F5F6EE', COPPER] if metric == 'temperature_c' else [COPPER, '#F5F6EE', GREEN]
        finite = pivot.to_numpy()[np.isfinite(pivot.to_numpy())]
        limit = max(float(np.max(np.abs(finite))) if finite.size else 1, 1)
        bounds = dict(zmin=-limit, zmax=limit, zmid=0)
    else:
        scale, bounds = ['#F3E9CA', '#A2BE8D', GREEN], {}
    fig = go.Figure(go.Heatmap(z=pivot.to_numpy(), x=[calendar.month_abbr[m] for m in range(1, 13)],
                              y=[str(y) for y in pivot.index], colorscale=scale, xgap=3, ygap=3,
                              colorbar=dict(title=label, thickness=10),
                              hovertemplate='%{x} %{y}<br>%{z:.2f} ' + label + '<extra></extra>', **bounds))
    fig.update_layout(yaxis=dict(type='category'))
    return style(fig, height=max(260, len(pivot) * 32 + 105))


def ranking(table, metric, unit):
    ranked = table.sort_values(metric)
    fig = go.Figure(go.Bar(y=ranked['name'], x=ranked[metric], orientation='h', marker_color=GREEN,
                           text=ranked[metric].round(1), textposition='outside',
                           hovertemplate='%{y}<br>%{x:.2f} ' + unit + '<extra></extra>'))
    fig.update_xaxes(title=unit)
    return style(fig, height=max(300, len(table) * 30 + 70))


def climate_map(table, metric, unit, assets=None, footprint=None, online=True):
    points = []
    minimum, maximum = float(table[metric].min()), float(table[metric].max())
    for row in table.to_dict('records'):
        ratio = (row[metric] - minimum) / (maximum - minimum) if maximum > minimum else .5
        color = [int(a + (b - a) * ratio) for a, b in zip([205, 185, 124], [25, 116, 91])]
        points.append({'lon': row['lon'], 'lat': row['lat'], 'color': color,
                       'radius': 1400 + 2800 * ratio, 'label': html.escape(row['name']),
                       'detail': f'{row[metric]:,.2f} {unit} · sampled climate grid cell'})
    layers = []
    if footprint:
        layers.append(pdk.Layer('GeoJsonLayer', footprint, filled=False, stroked=True,
                                 get_line_color=[95, 128, 100, 100], line_width_min_pixels=1, pickable=False))
    if assets is not None:
        layers.append(pdk.Layer('ScatterplotLayer', points, get_position='[lon, lat]', get_radius=20000,
                                 get_fill_color=[25, 116, 91, 12], get_line_color=[25, 116, 91, 100],
                                 stroked=True, line_width_min_pixels=1, pickable=False))
        colors = {'water': [71, 132, 166], 'healthcare': [177, 107, 88], 'education': [182, 150, 75]}
        records = []
        for feature in assets:
            if feature.get('geometry', {}).get('type') != 'Point':
                continue
            lon, lat = feature['geometry']['coordinates']
            props = feature['properties']
            records.append({'lon': lon, 'lat': lat, 'color': colors.get(props['category'], [100, 120, 100]),
                            'label': html.escape(str(props.get('name', 'Mapped object'))[:180]),
                            'detail': html.escape(props['category'] + ' · OSM ' + props.get('osm_id', ''))})
        if records:
            layers.append(pdk.Layer('ScatterplotLayer', records, get_position='[lon, lat]',
                                     get_fill_color='color', get_radius=75, radius_min_pixels=3,
                                     radius_max_pixels=8, pickable=True, opacity=.8))
    layers.append(pdk.Layer('ScatterplotLayer', points, get_position='[lon, lat]', get_fill_color='color',
                             get_radius='radius', radius_min_pixels=6, radius_max_pixels=15,
                             stroked=True, get_line_color=[255, 255, 255], line_width_min_pixels=2, pickable=True))
    span = max(float(table.lat.max() - table.lat.min()), float(table.lon.max() - table.lon.min()) * .8)
    zoom = max(4.4, min(8.2, 7 - math.log2(span + .2)))
    basemap = {'map_provider': None}
    if online:
        basemap = {'map_provider': 'carto', 'map_style': 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json'}
    return pdk.Deck(layers=layers,
                     initial_view_state=pdk.ViewState(latitude=float(table.lat.mean()), longitude=float(table.lon.mean()), zoom=zoom),
                     tooltip={'html': '<b>{label}</b><br>{detail}', 'style': {'backgroundColor': '#19382E', 'color': 'white'}},
                     **basemap)
