-- Month-over-month change in average PM2.5 per city (window function: lag)
select city, month, pm25_month_avg,
       pm25_month_avg - lag(pm25_month_avg) over (partition by city order by month) as change_vs_prev_month
from city_monthly
order by city, month;
