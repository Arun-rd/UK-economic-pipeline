{{ config(materialized='view') }}
/*
  Silver view: expose silver table with business-friendly names
  and filter to valid rows only.
*/
select
    id                  as silver_id,
    dataset_id,
    series_id,
    indicator_name,
    period,
    period_date,
    value_raw,
    value_numeric,
    unit,
    is_valid,
    loaded_at,
    date_trunc('quarter', period_date)::date  as period_quarter,
    extract(year from period_date)::int       as period_year
from {{ source('silver', 'ons_indicators') }}
where is_valid = true
  and value_numeric is not null
