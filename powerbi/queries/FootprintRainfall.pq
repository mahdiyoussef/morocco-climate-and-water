let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="mart_spatial_rainfall"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"year", Int64.Type}, {"month", Int64.Type}, {"year_month", type text}, {"area_weighted_rainfall_mm", type number}, {"valid_grid_cells", Int64.Type}, {"approximate_coverage_km2", type number}}, "en-US")
in
    Typed
