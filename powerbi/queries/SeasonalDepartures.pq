let
    Source = PostgreSQL.Database(PostgresServer, PostgresDatabase, [CreateNavigationProperties=false]),
    Gold = Source{[Schema="gold", Item="mart_monthly_anomalies"]}[Data],
    Typed = Table.TransformColumnTypes(Gold, {{"site", type text}, {"year", Int64.Type}, {"month", Int64.Type}, {"year_month", type text}, {"rainfall_mm", type number}, {"baseline_rainfall_mm", type number}, {"rainfall_anomaly_pct", type number}, {"temperature_anomaly_c", type number}, {"soil_moisture_anomaly_pp", type number}, {"baseline_start_year", Int64.Type}, {"baseline_end_year", Int64.Type}}, "en-US")
in
    Typed
