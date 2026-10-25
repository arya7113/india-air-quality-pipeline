-- The 10 worst city-days of the year
select city, date, pm25_avg, n_stations,
       round(pm25_avg / 15.0, 1) as times_who_limit
from daily_city
order by pm25_avg desc
limit 10;
