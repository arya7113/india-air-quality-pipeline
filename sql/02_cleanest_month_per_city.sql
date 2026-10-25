-- For each city, which month was cleanest? (window function: rank months within each city)
with ranked as (
    select city, month, pm25_month_avg, days_above_who,
           row_number() over (partition by city order by pm25_month_avg asc) as rn
    from city_monthly
    where days_measured >= 15
)
select city, month, pm25_month_avg, days_above_who
from ranked
where rn = 1
order by pm25_month_avg;
