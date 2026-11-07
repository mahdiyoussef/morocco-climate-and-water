"""Shared typed Silver contracts, used by Spark and the local verification warehouse."""
TABLES = {
    'chirps_grid': ({'year': ('year', 'int'), 'month': ('month', 'int'), 'lat': ('lat', 'double'), 'lon': ('lon', 'double'), 'rainfall_mm': ('rainfall_mm', 'double')},
                    ['year', 'month', 'lat', 'lon'], 'year between 1981 and 2100 and month between 1 and 12 and lat between -90 and 90 and lon between -180 and 180 and rainfall_mm >= 0'),
    'chirps_sites': ({'site': ('site', 'string'), 'year': ('year', 'int'), 'month': ('month', 'int'), 'rainfall_mm': ('rainfall_mm', 'double'), 'chirps_lat': ('chirps_lat', 'double'), 'chirps_lon': ('chirps_lon', 'double')},
                     ['site', 'year', 'month'], "site <> '' and month between 1 and 12 and rainfall_mm >= 0"),
    'era5_hourly': ({'site': ('site', 'string'), 'observed_at': ('time', 'timestamp'), 'temperature_c': ('temperature_c', 'double'), 'soil_moisture': ('soil_moisture', 'double')},
                    ['site', 'observed_at'], "site <> '' and temperature_c between -60 and 65 and soil_moisture between 0 and 1"),
    'locations': ({'id': ('id', 'string'), 'name': ('name', 'string'), 'zone': ('zone', 'string'), 'lat': ('lat', 'double'), 'lon': ('lon', 'double'), 'era5_lat': ('era5_lat', 'double'), 'era5_lon': ('era5_lon', 'double')},
                  ['id'], "id <> '' and lat between -90 and 90 and lon between -180 and 180"),
    'osm_services': ({'site': ('site', 'string'), 'osm_id': ('osm_id', 'string'), 'category': ('category', 'string'), 'name': ('name', 'string'), 'lat': ('lat', 'double'), 'lon': ('lon', 'double')},
                     ['site', 'osm_id'], "site <> '' and category in ('water','healthcare','education') and lat between -90 and 90 and lon between -180 and 180"),
    'osm_roads': ({'site': ('site', 'string'), 'road_km': ('road_km', 'double')}, ['site'], "site <> '' and road_km >= 0"),
    'source_audit': ({'dataset': ('dataset', 'string'), 'year': ('year', 'int'), 'path': ('path', 'string'), 'bytes': ('bytes', 'bigint'), 'sha256': ('sha256', 'string')},
                     ['dataset', 'year'], 'bytes > 0 and length(sha256) = 64'),
}


def cast_query(table, engine='spark'):
    fields = TABLES[table][0]
    casts = []
    for name, (expression, kind) in fields.items():
        dtype = 'varchar' if kind == 'string' and engine != 'spark' else kind
        casts.append(f'try_cast({expression} as {dtype}) as {name}')
    return 'select ' + ', '.join(casts) + ', _ingested_at, _source_file, _row_hash from bronze.' + table


def validity(table):
    fields, _, condition = TABLES[table]
    checks = [f'{name} is not null' for name in fields]
    checks += [f'{name} between -1.0e308 and 1.0e308' for name, (_, dtype) in fields.items() if dtype == 'double']
    return ' and '.join(checks) + ' and (' + condition + ')'


def dedupe_query(table):
    fields, keys, _ = TABLES[table]
    keep = ', '.join(list(fields) + ['_ingested_at', '_source_file', '_row_hash'])
    return f'''select {keep} from (
      select *, row_number() over (partition by {','.join(keys)} order by _ingested_at desc, _source_file desc, _row_hash desc) as _rn
      from candidates where coalesce(({validity(table)}), false)
    ) ranked where _rn = 1'''
