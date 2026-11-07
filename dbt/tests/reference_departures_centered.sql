-- The mean same-month rainfall departure across reference years should be zero.
select site, month
from {{ ref('mart_monthly_anomalies') }}
where year between baseline_start_year and baseline_end_year
group by site, month
having abs(avg(rainfall_anomaly_pct)) > 0.00001
