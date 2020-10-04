with tab1 as (
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
),  -- tab1: generate lat_bins and long_bins

tab2 as (
  select DISTINCT(CLUSTER_START_SLICE_S) as interval_start,
         DATEADD('minute', 15, interval_start) as interval_end
  from
  (select CLUSTER_STARTED_UTC,
          time_slice(CLUSTER_STARTED_UTC, 15, 'minutes', 'START') as CLUSTER_START_SLICE_S,
          time_slice(CLUSTER_STARTED_UTC, 15, 'minutes', 'END')   as CLUSTER_START_SLICE_E,
          DWELL_TIME_MINUTES
   from "ANALYTICS"."DEV"."AZ_CLUSTERS_7_15_to_7_22"
   --where day(CLUSTER_STARTED_UTC) = '17'
   order by CLUSTER_STARTED_UTC)
   order by 1
),  -- tab2: generate 15-min time intervals

tab3 as (
  select *
  from tab1
  left join tab2
  where tab2.interval_start < tab1.CLUSTER_ENDED_UTC AND
        tab2.interval_end > tab1.CLUSTER_STARTED_UTC
  order by lat_bin, long_bin, latitude
), -- tab3: join tab1 & tab2 to expand on 15-min time intevals

tab4 as (
  select lat_bins,
         long_bins,
         interval_start,
         interval_end,
         avg(LATITUDE) as avg_lat,
         avg(LONGITUDE) as avg_lon,
         count(*) as density   -- number of people in the same lat-long-time bin
  from tab3
  group by lat_bins, long_bins, interval_start, interval_end
  order by interval_start
) -- calculate pupulation-level density

select *
from tab4
