let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="source_audit"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"dataset", type text}, {"year", Int64.Type}, {"path", type text}, {"bytes", Int64.Type}, {"sha256", type text}}, "en-US")
in
    Typed
