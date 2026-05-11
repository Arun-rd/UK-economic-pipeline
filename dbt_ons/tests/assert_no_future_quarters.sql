-- Gold: no future period_quarters
select * from {{ ref('gold_uk_economic_kpis') }}
where period_quarter > current_date
