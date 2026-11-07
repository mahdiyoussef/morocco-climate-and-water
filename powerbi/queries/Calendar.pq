let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="dim_month"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"year_month", type text}, {"year", Int64.Type}, {"month", Int64.Type}, {"month_name", type text}, {"month_start", type date}}, "en-US")
in
    Typed
