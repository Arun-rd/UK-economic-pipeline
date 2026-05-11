{{ config(materialized='table') }}

/*
  gold_uk_economic_kpis
  ----------------------
  Final gold mart: one row per indicator per quarter.
  Adds period-over-period change, rolling averages, and
  performance flags. This is the table Power BI connects to.

  Indicators:
    - CPIH Inflation Rate
    - UK Unemployment Rate
    - UK GDP (chained volume)
    - Producer Price Index
    - UK Nominal GDP
*/

with base as (
    select * from {{ ref('slv_ons_indicators') }}
),

quarterly as (
    select
        series_id,
        indicator_name,
        unit,
        period_quarter,
        period_year,
        -- Use average when multiple monthly obs fall in same quarter
        round(avg(value_numeric)::numeric, 4)       as value,
        count(*)                                     as obs_count
    from base
    group by series_id, indicator_name, unit, period_quarter, period_year
),

with_changes as (
    select
        *,
        -- Quarter-over-quarter absolute change
        round((value - lag(value) over (
            partition by series_id order by period_quarter
        ))::numeric, 4)                              as qoq_change,

        -- Quarter-over-quarter % change
        round(((value - lag(value) over (
            partition by series_id order by period_quarter
        )) / nullif(lag(value) over (
            partition by series_id order by period_quarter
        ), 0) * 100)::numeric, 2)                    as qoq_pct_change,

        -- 4-quarter rolling average (smoothed trend)
        round(avg(value) over (
            partition by series_id
            order by period_quarter
            rows between 3 preceding and current row
        )::numeric, 4)                               as rolling_4q_avg,

        -- Year-over-year change
        round((value - lag(value, 4) over (
            partition by series_id order by period_quarter
        ))::numeric, 4)                              as yoy_change,

        round(((value - lag(value, 4) over (
            partition by series_id order by period_quarter
        )) / nullif(lag(value, 4) over (
            partition by series_id order by period_quarter
        ), 0) * 100)::numeric, 2)                    as yoy_pct_change

    from quarterly
),

final as (
    select
        series_id,
        indicator_name,
        unit,
        period_quarter,
        period_year,
        value,
        qoq_change,
        qoq_pct_change,
        yoy_change,
        yoy_pct_change,
        rolling_4q_avg,
        obs_count,

        -- Simple trend flag
        case
            when qoq_change > 0  then 'Rising'
            when qoq_change < 0  then 'Falling'
            else 'Flat'
        end                                          as trend_direction,

        -- Inflation-specific flag
        case
            when series_id = 'L55O' and value > 2.0 then 'Above target'
            when series_id = 'L55O' and value <= 2.0 then 'At/below target'
            else null
        end                                          as inflation_status

    from with_changes
)

select * from final
order by series_id, period_quarter
