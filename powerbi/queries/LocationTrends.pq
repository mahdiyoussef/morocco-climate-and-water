let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="mart_site_trends"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"site", type text}, {"observation_years", Int64.Type}, {"first_year", Int64.Type}, {"last_year", Int64.Type}, {"rainfall_slope_mm_per_year", type number}, {"temperature_slope_c_per_decade", type number}}, "en-US")
in
    Typed
