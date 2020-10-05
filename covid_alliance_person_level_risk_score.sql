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
