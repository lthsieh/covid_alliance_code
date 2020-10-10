-- This query will generate person-level COVID risk score.
-- This table also contains information about home dwell percentage.
-- Author: Frank Hsieh 10/10/2020
create or replace table "SCRATCH"."ANALYTICS_DEV_SCRATCH"."PERSON-LEVEL_RISK_SCORE_EXCLUDE_HOME_FH" as (
with
-- generate lat_bins and long_bins
tab1 as (
  select width_bucket(LATITUDE,
                     (select MIN(LATITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"),
                     (select MAX(LATITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"),
                     floor(((select MAX(LATITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22") -
                            (select MIN(LATITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"))*60)
                     ) as lat_bins,  -- generate latitude bins
         width_bucket(LONGITUDE,
                     (select MIN(LONGITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"),
                     (select MAX(LONGITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"),
                     floor(((select MAX(LONGITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22") -
                            (select MIN(LONGITUDE) from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"))*60)
                     ) as long_bins,   -- generate longitude bins
         *
  from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"
  where dwell_time_minutes > 5
  order by lat_bins, long_bins
),
-- generate 15-min time intervals
tab2 as (
  select DISTINCT(CLUSTER_START_SLICE_S) as interval_start,
         DATEADD('minute', 15, interval_start) as interval_end
  from
  (select CLUSTER_STARTED_LOCAL,
          time_slice(CLUSTER_STARTED_LOCAL, 15, 'minutes', 'START') as CLUSTER_START_SLICE_S,
          time_slice(CLUSTER_STARTED_LOCAL, 15, 'minutes', 'END')   as CLUSTER_START_SLICE_E,
          DWELL_TIME_MINUTES
   from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"
   --where day(CLUSTER_STARTED_LOCAL) = '17'
   order by CLUSTER_STARTED_LOCAL)
   order by 1
),
-- get home_lat and home_lon for each person
tab7 as (
  select advertiser_id,
         avg(HOME_LAT) as home_lat,
         avg(HOME_LON) as home_lon
  from "ANALYTICS"."DEV"."HOME_WORK"
  where HOME_CONFIDENCE = 'HIGH'
  group by advertiser_id
),
-- add home_lat and home_lon
tab5 as (
  select a.*,
         b.HOME_LAT,
         b.HOME_LON
  from tab1 a
  left join tab7 b
  on a.ADVERTISER_ID = b.ADVERTISER_ID
),
-- calculat distance from home and add a boolean column indicating homeness
tab6 as (
  select *,
         (HAVERSINE( LATITUDE, LONGITUDE, HOME_LAT, HOME_LON)) as Km_from_home,
         CASE WHEN Km_from_home < 0.01 THEN TRUE::boolean
         ELSE FALSE::boolean
         END as home_cluster
  from tab5
  -- where advertiser_id = 'Q56N0PN4N3N6976844620NNQ530R172Q'
),
-- join tables to expand on 15-min time intevals
tab3 as (
  select *
  from tab6 a
  left join tab2 b
  where b.interval_start < a.CLUSTER_ENDED_LOCAL AND
        b.interval_end   > a.CLUSTER_STARTED_LOCAL
  order by lat_bins, long_bins, latitude
),
-- calculate pupulation-level density while excluding home clusters
tab4 as (
  select lat_bins,
         long_bins,
         interval_start,
         interval_end,
         avg(LATITUDE) as avg_lat,
         avg(LONGITUDE) as avg_lon,
         count(*) as density   -- number of people in the same lat-long-time bin
  from tab3
  where home_cluster = FALSE  -- remove home clusters
  group by lat_bins, long_bins, interval_start, interval_end
  order by interval_start
),
-- identify the most crowded spatio-temproal cluster and get advertiser_ids in that cluster
tab8 as (
  select a.*,
         b.density
  from (select * from tab3 where home_cluster = FALSE) a
  right join (select lat_bins, long_bins, interval_start, interval_end, density
              from tab4
              where density = (select max(density) from tab4)
              ) b
  on a.lat_bins = b.lat_bins and
     a.long_bins = b.long_bins and
     a.interval_start = b.interval_start and
     a.interval_end = b.interval_end
),
-- calculated individual-level risk score while excluding home clusters
tab9 as (
  select advertiser_id,
         date_trunc('DAY', a.interval_start) as y_m_d,
         count(density)*15 as minutes_share_STcluster,
         max(density) as max_numofppl_share_STcluster,
         min(density) as min_numofppl_share_STcluster,
         avg(density) as avg_numofppl_share_STcluster
  from (select * from tab3 where home_cluster = FALSE) a  -- remove home-dwell clusters
  left join tab4 b
  on a.lat_bins = b.lat_bins and
     a.long_bins = b.long_bins and
     a.interval_start = b.interval_start
  group by advertiser_id, y_m_d
  order by advertiser_id, y_m_d
),
-- add home_dwell_% to individual-level risk table
tab10 as (
  select b.*,
         a.HOME_DWELL_PERCENT,
         a.HOME_DWELL_TIME_MINUTES,
         a.TOTAL_DWELL_TIME_MINUTES,
         a.HOME_CONFIDENCE,
         a.HOME_LAT,
         a.HOME_LON
  from "ANALYTICS"."DEV"."HOME_DWELL_PERCENTAGE" a
  right join tab9 b
  on a.advertiser_id = b.advertiser_id and
     date_trunc('DAY', a.date) = b.Y_M_D
)

select *
from tab10
)
