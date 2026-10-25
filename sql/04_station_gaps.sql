-- Data quality: which stations have the worst coverage? (months with < 20 complete days)
select city, station_name, month, days_with_data, complete_days
from station_coverage
where complete_days < 20
order by complete_days asc, city
limit 20;
