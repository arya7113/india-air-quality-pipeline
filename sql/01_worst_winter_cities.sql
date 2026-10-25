-- Which cities had the most days above the WHO PM2.5 limit in winter (Nov-Feb)?
select city,
       sum(days_measured)  as days_measured,
       sum(days_above_who) as days_above_who,
       round(100.0 * sum(days_above_who) / sum(days_measured), 1) as pct_days_above_who,
       round(avg(pm25_month_avg), 1) as avg_pm25
from city_monthly
where month in (11, 12, 1, 2)
group by city
having sum(days_measured) >= 30          -- ignore cities with almost no winter data
order by pct_days_above_who desc, avg_pm25 desc
limit 10;
