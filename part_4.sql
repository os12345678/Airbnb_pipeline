-- What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “neighbourhood_cleansed” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 
with num_stays as (
select 
    distinct(neighbourhood_cleansed) as neighbourhood,
    availability_30,
    ((30-availability_30)*try_cast(replace(trim(price,'$'), ',','') as numeric)) as est_revenue_per_active_listing,
    median_age_persons
from data_warehouse.listing_final
where has_availability='t'
and neighbourhood_cleansed is not null
group by 1,2,3,4)
select 
    neighbourhood, avg(est_revenue_per_active_listing) as avg_est_rev,
    median_age_persons as med_age
from num_stays
group by neighbourhood, med_age
order by avg_est_rev desc;



--What will be the best type of listing (property type, room type and accommodates for) for the top 5 “neighbourhood_cleansed” (in terms of estimated revenue per active listing) to have the highest number of stays?
with cte as (
select 
    distinct(neighbourhood_cleansed) as neighbourhood,
    availability_30,
    try_cast(replace(trim(price,'$'), ',','') as numeric) as price,
    property_type, room_type, accommodates,
    round(avg((30-availability_30)*try_cast(replace(trim(price,'$'), ',','') as numeric)),3) as est_revenue_per_active_listing
from data_warehouse.listing_final
where has_availability='t'
and neighbourhood_cleansed is not null
group by 1,2,3,4,5,6)
select distinct 
    neighbourhood, est_revenue_per_active_listing, 
    property_type, room_type, accommodates 
from cte
order by est_revenue_per_active_listing desc
limit 5;


-- Do hosts with multiple listings are more inclined to have their listings in the same “neighbourhood” as where they live? 
-- no, only a small portion of hosts with multiple listings have their listings in the same neighbourhood as where they live. 101/3700
with cte as ( 
select
    distinct(neighbourhood_cleansed) as neighbourhood, host_location,
    sum(case when host_is_superhost = 't' then 1 else 0 end) as total_superhost,
    count(distinct(id)) as total_hosts
from data_warehouse.listing_final
where neighbourhood_cleansed is not null
group by 1,2
)
select neighbourhood as listing_neighbourhood, split_part(host_location, ',', 0) as host_neighbourhood,
case
    WHEN listing_neighbourhood = host_neighbourhood THEN 1 ELSE 0 
end as same_neighbourhood
from cte
;


-- For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “neighbourhood_cleansed”?
with cte as (
select 
    distinct(host_id) as total_distinct_hosts,
    neighbourhood_cleansed,
    round(avg((30-availability_30)*try_cast(replace(trim(price,'$'), ',','') as numeric)),3) as est_revenue_month,
    MEDIAN_MORTGAGE_REPAY_MONTHLY as med_mortgage_month
from data_warehouse.listing_final
where has_availability = 't'
group by 1,2,4
)
select *,
case when (est_revenue_month > med_mortgage_month) then 1 else 0 end as rev_vs_mortgage
from cte;

















