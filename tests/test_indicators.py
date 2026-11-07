"""Verify annual indicator semantics and undefined ratios, independent of UI layout."""
import math
import pandas as pd
import pytest
from dashboard.indicators import annual_profiles


def observations():
    fact = pd.DataFrame([{'site':'x','year':year,'month':month,'rainfall_mm':rain}
                         for year,rain,months in [(2020,1.,12),(2021,2.,12),(2022,1000.,11)]
                         for month in range(1,months+1)])
    anomalies = fact.assign(baseline_rainfall_mm=1.5, temperature_c=2., baseline_temperature_c=1.,
                            soil_moisture=.2, baseline_soil_moisture=.3)
    return fact, anomalies


def test_profiles_exclude_incomplete_years_and_use_sample_cv():
    fact, anomalies = observations()
    row = annual_profiles(fact, anomalies).iloc[0]
    assert row.complete_years == 2 and row.eligible_months == 24
    assert row.mean_annual_rainfall_mm == 18
    assert row.rainfall_cv_pct == pytest.approx(100 * math.sqrt(72) / 18)
    assert row.wettest_three_month_share_pct == 25
    assert row.warm_dry_months_pct == 50
    assert row.below_reference_moisture_months_pct == 100


def test_zero_rainfall_and_one_year_do_not_manufacture_ratios():
    fact, anomalies = observations()
    fact = fact[fact.year.eq(2020)].assign(rainfall_mm=0.)
    anomalies = anomalies[anomalies.year.eq(2020)].assign(rainfall_mm=0., baseline_rainfall_mm=0.)
    row = annual_profiles(fact, anomalies).iloc[0]
    assert pd.isna(row.rainfall_cv_pct) and pd.isna(row.wettest_three_month_share_pct)
    assert row.warm_dry_months_pct == 0


def test_missing_reference_month_is_excluded_from_frequency_denominator():
    fact, anomalies = observations()
    anomalies.loc[0,'baseline_temperature_c'] = float('nan')
    row = annual_profiles(fact, anomalies).iloc[0]
    assert row.eligible_months == 23
    assert row.warm_dry_months_pct == pytest.approx(100 * 11 / 23)
