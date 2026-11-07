let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="dim_location"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"id", type text}, {"name", type text}, {"zone", type text}, {"lat", type number}, {"lon", type number}}, "en-US")
in
    Typed
