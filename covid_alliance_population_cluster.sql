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
