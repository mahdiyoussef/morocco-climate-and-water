let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="mart_water_year"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"site", type text}, {"water_year", Int64.Type}, {"months_covered", Int64.Type}, {"is_complete", type logical}, {"rainfall_mm", type number}, {"temperature_c", type number}, {"soil_moisture_pct", type number}}, "en-US")
in
    Typed
