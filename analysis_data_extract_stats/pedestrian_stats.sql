-- These queries are formulated to derive the expected stats

-- Top 10 (most pedestrians) locations by day
select Day,location from aggregated_db.all_count_location_by_day_agg order by sum_counts desc limit 10;

-- Top 10 (most pedestrians) locations per day in a week
-- We have used a window function ROW_NUMBER() to generate the rankings of each location per day based on the pedestrian counts and then filtered top 10 rankings per day
with t1 as(
as
(select Day,location,sum_counts,ROW_NUMBER() over (partition by Day order by sum_counts desc ) as rank from aggregated_db.all_count_location_by_day_agg)
select * from t1 where rank<=10 order by day,rank;


-- Top 10 (most pedestrians) locations by Month

select Day,location from aggregated_db.all_count_location_by_month_agg order by sum_counts desc limit 10;

-- Top 10 (most pedestrians) locations per Month in all years
-- We have used a window function ROW_NUMBER() to generate the rankings of each location per Month based on the pedestrian counts and then filtered top 10 rankings per Month
with t1 as(
as
(select Month,location,sum_counts,ROW_NUMBER() over (partition by Month order by sum_counts desc ) as rank from aggregated_db.all_count_location_by_month_agg)
select * from t1 where rank<=10 order by Month,rank;

--Which location has shown most decline due to lockdowns in last 2 years
-- we have used left join on the same table to derive the stats for every year compared to its previous year.Then we have aggregated sum for every location with growth total of last 2 years from current year
with t1 as (
select c1.*,((c1.sum_counts - c2.sum_counts) / c2.sum_counts) * 100 as growth_per from aggregated_db.all_count_location_by_year_agg c1 left join aggregated_db.all_count_location_by_year_agg c2 on c1.location = c2.location and c1.Year = c2.Year + 1 order by year,location
)
--is not null condition used to exclude the locations where the last 2 year data not available as these new locations were added from 2022
select location,sum(growth_per) as all_growth from t1 where year>=EXTRACT(YEAR FROM CURRENT_DATE)-2 and growth_per is not null group by location order by all_growth limit 1;


--Which location has most growth in last year
with t1 as (
select c1.*,((c1.sum_counts - c2.sum_counts) / c2.sum_counts) * 100 as growth_per from aggregated_db.all_count_location_by_year_agg c1 left join aggregated_db.all_count_location_by_year_agg c2 on c1.location = c2.location and c1.Year = c2.Year + 1 order by year,location
)
select location,sum(growth_per) as all_growth from t1 where year>=EXTRACT(YEAR FROM CURRENT_DATE)-1 and growth_per is not null group by location order by all_growth desc limit 1;
