{{ config(materialized='view') }}
-- Bronze view: expose raw table with basic column aliasing
select
    id                  as bronze_id,
    dataset_id,
    series_id,
    period,
    value               as raw_value,
    raw_json,
    ingested_at
from {{ source('bronze', 'ons_raw') }}
