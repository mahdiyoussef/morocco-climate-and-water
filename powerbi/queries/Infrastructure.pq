let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="fact_infrastructure"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"site", type text}, {"healthcare", Int64.Type}, {"education", Int64.Type}, {"water", Int64.Type}, {"road_km", type number}}, "en-US")
in
    Typed
