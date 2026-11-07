let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="fact_climate_monthly"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"site", type text}, {"year", Int64.Type}, {"month", Int64.Type}, {"year_month", type text}, {"rainfall_mm", type number}, {"temperature_c", type number}, {"soil_moisture", type number}, {"hours", Int64.Type}, {"chirps_lat", type number}, {"chirps_lon", type number}, {"era5_lat", type number}, {"era5_lon", type number}}, "en-US")
in
    Typed
