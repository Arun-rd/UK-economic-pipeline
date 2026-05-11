-- No quarter-over-quarter change > 50% (data quality guard)
select * from {{ ref('gold_uk_economic_kpis') }}
where abs(qoq_pct_change) > 50
  and qoq_pct_change is not null
