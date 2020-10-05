# covid_alliance_code

This repo contains the SQL code, as part of the deliverables to COVID Alliance, to process and compute person-level metrics for COVID-19 risk based on mobile devices geolocation data for over 25 million Americans from mid January, 2020 to today. Currently there are over 400 billion rows of data and new data is ingested into the Snowflake DB on a daily basis. 

The mobility metric derived from these scripts seeks to improve data and models related to COVID-19. The SQL scripts implement an algorithm that allows us to generate a daily person-level risk score for each individual on the basis of her/his mobility patterns on a given day. The derived person-level mobility metric outperformed existing metrics - daily distance travelled and the share of stationary time spent at home - which are known to predict COVID-19 spread, in capturing much of the richness in the geolocation data. 
