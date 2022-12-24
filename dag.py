import os
import logging 
import requests
import pandas as pd
from datetime import datetime, timedelta
import glob 
from psycopg2.extras import execute_values
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


##################################################
#
#     Load Environment variables
#
##################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

##################################################
#
#     DAG settings
#
##################################################

dag_default_args = {
    'owner':'Assignment_3',
    'start_date':datetime.now(),
    'email':[],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'depends_on_past':False,
    'wait_for_downstream':False,
}

dag = DAG(
    dag_id='Assignment_3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
) 

##################################################
#
#     Custom logics for operator
#
##################################################

create_raw = f"""
CREATE OR REPLACE DATABASE assignment_3;
USE assignment_3;

CREATE OR REPLACE SCHEMA raw;
USE assignment_3.raw;

CREATE OR REPLACE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://australia-southeast1-bde-12b59a40-bucket/dags/data');

DESCRIBE INTEGRATION GCP;

CREATE OR REPLACE STAGE stage_gcp
storage_integration = GCP
url='gcs://australia-southeast1-bde-12b59a40-bucket/dags/data';

list @stage_gcp;
CREATE OR REPLACE FILE FORMAT file_format_csv
type = 'CSV'
field_delimiter = ','
skip_header = 0
NULL_IF = ('\\N','NULL', 'N/A', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE EXTERNAL TABLE raw.listings_7
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*/listings/0[7]_2020[.]csv';
select * from listings_7;

CREATE OR REPLACE EXTERNAL TABLE raw.listings_5_6
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*/listings/0[5 | 6]_2020[.]csv';
select * from raw.listings_5_6 limit 1;

CREATE OR REPLACE EXTERNAL TABLE raw.listings_rest
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*/listings/.*[.]csv';
select COUNT(*) from raw.listings_rest;

CREATE OR REPLACE EXTERNAL TABLE raw.censusG01
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*Census LGA/2016Census_G01_NSW_LGA.csv';
select count(*) from raw.censusg01;

CREATE OR REPLACE EXTERNAL TABLE raw.censusG02
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*Census LGA/2016Census_G02_NSW_LGA.csv';
select count(*) from raw.censusg02;

CREATE OR REPLACE EXTERNAL TABLE raw.join_census
with location = @stage_gcp
file_format = file_format_csv
pattern = '.*data/LGA_2016_NSW.csv';
select count(*) from raw.join_census;

"""

refresh_census = f"""
CREATE OR REPLACE SCHEMA staging;
ALTER EXTERNAL TABLE raw.censusG01 REFRESH;
CREATE OR REPLACE TABLE staging.staging_censusG01 as
SELECT
    value:c1::varchar as LGA_CODE_2016,
    value:c2::varchar as Tot_P_M,
    value:c100::varchar as High_yr_schl_comp_Yr_8_belw_P,
    value:c101::varchar as High_yr_schl_comp_D_n_g_sch_M,
    value:c102::varchar as High_yr_schl_comp_D_n_g_sch_F,
    value:c103::varchar as High_yr_schl_comp_D_n_g_sch_P,
    value:c104::varchar as Count_psns_occ_priv_dwgs_M,
    value:c105::varchar as Count_psns_occ_priv_dwgs_F,
    value:c106::varchar as Count_psns_occ_priv_dwgs_P,
    value:c107::varchar as Count_Persons_other_dwgs_M,
    value:c108::varchar as Count_Persons_other_dwgs_F,
    value:c109::varchar as Count_Persons_other_dwgs_P,
    value:c11::varchar as Age_15_19_yr_M,
    value:c12::varchar as Age_15_19_yr_F,
    value:c13::varchar as Age_15_19_yr_P,
    value:c14::varchar as Age_20_24_yr_M,
    value:c15::varchar as Age_20_24_yr_F,
    value:c16::varchar as Age_20_24_yr_P,
    value:c17::varchar as Age_25_34_yr_M,
    value:c18::varchar as Age_25_34_yr_F,
    value:c19::varchar as Age_25_34_yr_P,
    value:c20::varchar as Age_35_44_yr_M,
    value:c21::varchar as Age_35_44_yr_F,
    value:c22::varchar as Age_35_44_yr_P,
    value:c23::varchar as Age_45_54_yr_M, 
    value:c24::varchar as Age_45_54_yr_F, 
    value:c25::varchar as Age_45_54_yr_P, 
    value:c26::varchar as Age_55_64_yr_M,
    value:c27::varchar as Age_55_64_yr_F,
    value:c28::varchar as Age_55_64_yr_P,
    value:c29::varchar as Age_65_74_yr_M,
    value:c3::varchar as Tot_P_F,
    value:c30::varchar as Age_65_74_yr_F,
    value:c31::varchar as Age_65_74_yr_P,
    value:c32::varchar as Age_75_84_yr_M,
    value:c33::varchar as Age_75_84_yr_F,
    value:c34::varchar as Age_75_84_yr_P,
    value:c35::varchar as Age_85ov_M,
    value:c36::varchar as Age_85ov_F,
    value:c37::varchar as Age_85ov_P,
    value:c38::varchar as Counted_Census_Night_home_M,
    value:c39::varchar as Counted_Census_Night_home_F,
    value:c40::varchar as Counted_Census_Night_home_P,
    value:c41::varchar as Count_Census_Nt_Ewhere_Aust_M,
    value:c42::varchar as Count_Census_Nt_Ewhere_Aust_F,
    value:c43::varchar as Count_Census_Nt_Ewhere_Aust_P,
    value:c44::varchar as Indigenous_psns_Aboriginal_M,
    value:c45::varchar as Indigenous_psns_Aboriginal_F,
    value:c46::varchar as Indigenous_psns_Aboriginal_P,
    value:c47::varchar as Indig_psns_Torres_Strait_Is_M,
    value:c48::varchar as Indig_psns_Torres_Strait_Is_F,
    value:c49::varchar as Indig_psns_Torres_Strait_Is_P,
    value:c5::varchar as Age_0_4_yr_M,
    value:c50::varchar as Indig_Bth_Abor_Torres_St_Is_M,
    value:c51::varchar as Indig_Bth_Abor_Torres_St_Is_F,
    value:c52::varchar as Indig_Bth_Abor_Torres_St_Is_P,
    value:c53::varchar as Indigenous_P_Tot_M,
    value:c54::varchar as Indigenous_P_Tot_F,
    value:c55::varchar as Indigenous_P_Tot_P,
    value:c56::varchar as Birthplace_Australia_M,
    value:c57::varchar as Birthplace_Australia_F,
    value:c58::varchar as Birthplace_Australia_P,
    value:c59::varchar as Birthplace_Elsewhere_M,
    value:c6::varchar as Age_0_4_yr_F,
    value:c60::varchar as Birthplace_Elsewhere_F,
    value:c61::varchar as Birthplace_Elsewhere_P,
    value:c62::varchar as Lang_spoken_home_Eng_only_M,
    value:c63::varchar as Lang_spoken_home_Eng_only_F,
    value:c64::varchar as Lang_spoken_home_Eng_only_P,
    value:c65::varchar as Lang_spoken_home_Oth_Lang_M,
    value:c66::varchar as Lang_spoken_home_Oth_Lang_F,
    value:c67::varchar as Lang_spoken_home_Oth_Lang_P,
    value:c68::varchar as Australian_citizen_M,
    value:c69::varchar as Australian_citizen_F,
    value:c7::varchar as Age_0_4_yr_P,
    value:c70::varchar as Australian_citizen_P,
    value:c71::varchar as Age_psns_att_educ_inst_0_4_M,
    value:c72::varchar as Age_psns_att_educ_inst_0_4_F,
    value:c73::varchar as Age_psns_att_educ_inst_0_4_P,
    value:c74::varchar as Age_psns_att_educ_inst_5_14_M,
    value:c75::varchar as Age_psns_att_educ_inst_5_14_F,
    value:c76::varchar as Age_psns_att_educ_inst_5_14_P,
    value:c77::varchar as Age_psns_att_edu_inst_15_19_M,
    value:c78::varchar as Age_psns_att_edu_inst_15_19_F,
    value:c79::varchar as Age_psns_att_edu_inst_15_19_P,
    value:c8::varchar as Age_5_14_yr_M,
    value:c80::varchar as Age_psns_att_edu_inst_20_24_M,
    value:c81::varchar as Age_psns_att_edu_inst_20_24_F,
    value:c82::varchar as Age_psns_att_edu_inst_20_24_P,
    value:c83::varchar as Age_psns_att_edu_inst_25_ov_M,
    value:c84::varchar as Age_psns_att_edu_inst_25_ov_F,
    value:c85::varchar as Age_psns_att_edu_inst_25_ov_P,
    value:c86::varchar as High_yr_schl_comp_Yr_12_eq_M,
    value:c87::varchar as High_yr_schl_comp_Yr_12_eq_F,
    value:c88::varchar as High_yr_schl_comp_Yr_12_eq_P,
    value:c89::varchar as High_yr_schl_comp_Yr_11_eq_M,
    value:c9::varchar as Age_5_14_yr_F,
    value:c10::varchar as Age_5_14_yr_P,
    value:c90::varchar as High_yr_schl_comp_Yr_11_eq_F,
    value:c91::varchar as High_yr_schl_comp_Yr_11_eq_P,
    value:c92::varchar as High_yr_schl_comp_Yr_10_eq_M,
    value:c93::varchar as High_yr_schl_comp_Yr_10_eq_F,
    value:c94::varchar as High_yr_schl_comp_Yr_10_eq_P,
    value:c95::varchar as High_yr_schl_comp_Yr_9_eq_M,
    value:c96::varchar as High_yr_schl_comp_Yr_9_eq_F,
    value:c97::varchar as High_yr_schl_comp_Yr_9_eq_P,
    value:c98::varchar as High_yr_schl_comp_Yr_8_belw_M,
    value:c99::varchar as High_yr_schl_comp_Yr_8_belw_F
FROM raw.censusG01;

ALTER EXTERNAL TABLE raw.censusG02 REFRESH;
CREATE OR REPLACE TABLE staging.staging_censusG02 as
SELECT
    value:c1::varchar as LGA_CODE_2016,
    value:c2::varchar as Median_age_persons,
    value:c3::varchar as Median_mortgage_repay_monthly,
    value:c4::varchar as Median_tot_prsnl_inc_weekly,
    value:c5::varchar as Median_rent_weekly, 
    value:c6::varchar as Median_tot_fam_inc_weekly,
    value:c7::varchar as Average_num_psns_per_bedroom,
    value:c8::varchar as Median_tot_hhd_inc_weekly,
    value:c9::varchar as Average_household_size
FROM raw.censusg02;

ALTER EXTERNAL TABLE raw.join_census REFRESH;
CREATE OR REPLACE TABLE staging.staging_join_census as
SELECT 
    CONCAT('LGA', value:c1::varchar) as LGA_CODE,
    split_part(value:c2::varchar, ' (', 1) as LGA_NAME
FROM raw.join_census;

"""

refresh_listing = f"""
ALTER EXTERNAL TABLE raw.listings_5_6 REFRESH;
CREATE OR REPLACE TABLE staging.listing_5_6 as 
SELECT 
    value:c1::varchar as id,
    value:c2::varchar as listing_url,
    value:c3::varchar as scrape_id,
    value:c4::varchar as last_scraped,
    value:c5::varchar as name,
    value:c8::varchar as description,
    value:c10::varchar as neighbourhood_overview,
    value:c18::varchar as picture_url,
    value:c20::varchar as host_id,
    value:c21::varchar as host_url,
    value:c22::varchar as host_name,
    value:c23::varchar as host_since,
    value:c24::varchar as host_location,
    value:c25::varchar as host_about,
    value:c26::varchar as host_response_time,
    value:c27::varchar as host_response_rate,
    value:c28::varchar as host_acceptance_rate,
    value:c29::varchar as host_is_superhost,
    value:c30::varchar as host_thumbnail_url,
    value:c31::varchar as host_picture_url,
    value:c32::varchar as host_neighbourhood,
    value:c33::varchar as host_listings_count,
    value:c34::varchar as host_total_listings_count,
    value:c35::varchar as host_verifications,
    value:c36::varchar as host_has_profile_pic,
    value:c37::varchar as host_identity_verified,
    value:c39::varchar as neighbourhood,
    value:c40::varchar as neighbourhood_cleansed,
    value:c41::varchar as neighbourhood_group_cleansed,
    value:c49::varchar as latitude,
    value:c50::varchar as longitude,
    value:c52::varchar as property_type,
    value:c53::varchar as room_type,
    value:c54::varchar as accommodates,
    value:c55::varchar as bathrooms,
    value:c61::varchar as price,
    value:c68::varchar as minimum_minimum_nights,
    value:c69::varchar as maximum_minimum_nights,
    value:c70::varchar as minimum_maximum_nights,
    value:c71::varchar as maximum_maximum_nights,
    value:c72::varchar as minimum_nights_avg_ntm,
    value:c73::varchar as maximum_nights_avg_ntm,
    value:c76::varchar as calendar_updated,
    value:c77::varchar as has_availability,
    value:c78::varchar as availability_30,
    value:c79::varchar as availability_60,
    value:c80::varchar as availability_90,
    value:c81::varchar as availability_365,
    value:c82::varchar as calendar_last_scraped,
    value:c83::varchar as number_of_reviews,
    value:c84::varchar as number_of_reviews_ltm,
    value:c85::varchar as first_review,
    value:c86::varchar as last_review,
    value:c87::varchar as review_scores_rating,
    value:c88::varchar as review_scores_accuracy,
    value:c89::varchar as review_scores_cleanliness,
    value:c90::varchar as experiences_offered,
    value:c91::varchar as review_scores_checkin,
    value:c92::varchar as review_scores_communication,
    value:c93::varchar as review_scores_location,
    value:c94::varchar as review_scores_value,
    value:c95::varchar as license,
    value:c97::varchar as instant_bookable,
    value:c102::varchar as calculated_host_listings_count,
    value:c103::varchar as calculated_host_listings_count_entire_homes,
    value:c104::varchar as calculated_host_listings_count_private_rooms,
    value:c105::varchar as calculated_host_listings_count_shared_rooms,
    value:c106::varchar as reviews_per_month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 1)::varchar as month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 2)::varchar as year
FROM raw.listings_5_6;

ALTER EXTERNAL TABLE raw.listings_rest REFRESH;
CREATE OR REPLACE TABLE staging.listing_rest AS
SELECT 
    value:c1::varchar AS id,
    value:c2::varchar AS listing_url,
    value:c3::varchar AS scrape_id,
    value:c4::varchar AS last_scraped,
    value:c5::varchar AS name,
    value:c6::varchar AS description,
    value:c7::varchar AS neighbourhood_overview,
    value:c8::varchar AS picture_url,
    value:c9::varchar AS host_id,
    value:c10::varchar AS host_url,
    value:c11::varchar AS host_name,
    value:c12::varchar AS host_since,
    value:c13::varchar AS host_location,
    value:c14::varchar AS host_about,
    value:c15::varchar AS host_response_time,
    value:c16::varchar AS host_response_rate,
    value:c17::varchar AS host_acceptance_rate,
    value:c18::varchar AS host_is_superhost,
    value:c19::varchar AS host_thumbnail_url,
    value:c20::varchar AS host_picture_url,
    value:c21::varchar AS host_neighbourhood,
    value:c22::varchar AS host_listings_count,
    value:c23::varchar AS host_total_listings_count,
    value:c24::varchar AS host_verifications,
    value:c25::varchar AS host_has_profile_pic,
    value:c26::varchar AS host_identity_verified,
    value:c27::varchar AS neighbourhood,
    value:c28::varchar AS neighbourhood_cleansed,
    value:c29::varchar AS neighbourhood_group_cleansed,
    value:c30::varchar AS latitude,
    value:c31::varchar AS longitude,
    value:c32::varchar AS property_type,
    value:c33::varchar AS room_type,
    value:c34::varchar AS accommodates,
    value:c35::varchar AS bathrooms,
    value:c40::varchar AS price,
    value:c43::varchar AS minimum_minimum_nights,
    value:c44::varchar AS maximum_minimum_nights,
    value:c45::varchar AS minimum_maximum_nights,
    value:c46::varchar AS maximum_maximum_nights,
    value:c47::varchar AS minimum_nights_avg_ntm,
    value:c48::varchar AS maximum_nights_avg_ntm,
    value:c49::varchar AS calendar_updated,
    value:c50::varchar AS has_availability,
    value:c51::varchar AS availability_30,
    value:c52::varchar AS availability_60,
    value:c53::varchar AS availability_90,
    value:c54::varchar AS availability_365,
    value:c55::varchar AS calendar_last_scraped,
    value:c56::varchar AS number_of_reviews,
    value:c57::varchar AS number_of_reviews_ltm,
    value:c59::varchar AS first_review,
    value:c60::varchar AS last_review,
    value:c61::varchar AS review_scores_rating,
    value:c62::varchar AS review_scores_accuracy,
    value:c63::varchar AS review_scores_cleanliness,
    value:c64::varchar AS review_scores_checkin,
    value:c65::varchar AS review_scores_communication,
    value:c66::varchar AS review_scores_location,
    value:c67::varchar AS review_scores_value,
    value:c68::varchar AS license,
    value:c69::varchar AS instant_bookable,
    value:c70::varchar AS calculated_host_listings_count,
    value:c71::varchar AS calculated_host_listings_count_entire_homes,
    value:c72::varchar AS calculated_host_listings_count_private_rooms,
    value:c73::varchar AS calculated_host_listings_count_shared_rooms,
    value:c74::varchar AS reviews_per_month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 1)::varchar as month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 2)::varchar as year
FROM raw.listings_rest;

ALTER EXTERNAL TABLE raw.listings_7 REFRESH;
CREATE OR REPLACE TABLE staging.listing_7 as 
SELECT 
    value:c1::varchar AS id,
    value:c2::varchar AS listing_url,
    value:c3::varchar AS scrape_id,
    value:c4::varchar AS last_scraped,
    value:c5::varchar AS name,
    value:c6::varchar AS description,
    value:c7::varchar AS neighbourhood_overview,
    value:c8::varchar AS picture_url,
    value:c9::varchar AS host_id,
    value:c10::varchar AS host_url,
    value:c11::varchar AS host_name,
    value:c12::varchar AS host_since,
    value:c13::varchar AS host_location,
    value:c14::varchar AS host_about,
    value:c15::varchar AS host_response_time,
    value:c16::varchar AS host_response_rate,
    value:c17::varchar AS host_acceptance_rate,
    value:c18::varchar AS host_is_superhost,
    value:c19::varchar AS host_thumbnail_url,
    value:c20::varchar AS host_picture_url,
    value:c21::varchar AS host_neighbourhood,
    value:c22::varchar AS host_listings_count,
    value:c23::varchar AS host_total_listings_count,
    value:c24::varchar AS host_verifications,
    value:c25::varchar AS host_has_profile_pic,
    value:c26::varchar AS host_identity_verified,
    value:c27::varchar AS neighbourhood,
    value:c28::varchar AS neighbourhood_cleansed,
    value:c29::varchar AS neighbourhood_group_cleansed,
    value:c30::varchar AS latitude,
    value:c31::varchar AS longitude,
    value:c32::varchar AS property_type,
    value:c33::varchar AS room_type,
    value:c34::varchar AS accommodates,
    value:c35::varchar AS bathrooms,
    value:c58::varchar AS price,
    value:c65::varchar AS minimum_minimum_nights,
    value:c66::varchar AS maximum_minimum_nights,
    value:c67::varchar AS minimum_maximum_nights,
    value:c68::varchar AS maximum_maximum_nights,
    value:c69::varchar AS minimum_nights_avg_ntm,
    value:c70::varchar AS maximum_nights_avg_ntm,
    value:c71::varchar AS calendar_updated,
    value:c72::varchar AS has_availability,
    value:c73::varchar AS availability_30,
    value:c74::varchar AS availability_60,
    value:c75::varchar AS availability_90,
    value:c76::varchar AS availability_365,
    value:c77::varchar AS calendar_last_scraped,
    value:c78::varchar AS number_of_reviews,
    value:c79::varchar AS number_of_reviews_ltm,
    value:c81::varchar AS first_review,
    value:c82::varchar AS last_review,
    value:c83::varchar AS review_scores_rating,
    value:c84::varchar AS review_scores_accuracy,
    value:c85::varchar AS review_scores_cleanliness,
    value:c86::varchar AS review_scores_checkin,
    value:c87::varchar AS review_scores_communication,
    value:c88::varchar AS review_scores_location,
    value:c89::varchar AS review_scores_value,
    value:c90::varchar AS license,
    value:c91::varchar AS instant_bookable,
    value:c92::varchar AS calculated_host_listings_count,
    value:c99::varchar AS calculated_host_listings_count_entire_homes,
    value:c100::varchar AS calculated_host_listings_count_private_rooms,
    value:c101::varchar AS calculated_host_listings_count_shared_rooms,
    value:c102::varchar AS reviews_per_month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 1)::varchar as month,
    split_part(split_part((split_part(metadata$filename, '/', -1)::varchar), '.', 1)::varchar, '_', 2)::varchar as year
FROM raw.listings_7;

"""

merge_schema = f"""
CREATE OR REPLACE SCHEMA data_warehouse;
CREATE OR REPLACE VIEW data_warehouse.census as 
SELECT *
FROM staging.staging_censusg01 a 
INNER JOIN staging.staging_censusg02 b using (LGA_CODE_2016);

CREATE OR REPLACE TABLE data_warehouse.census_agg as 
SELECT *
FROM data_warehouse.census a
INNER JOIN staging.staging_join_census b 
ON a.lga_code_2016 = b.lga_code;

CREATE OR REPLACE TABLE data_warehouse.listings_agg AS
SELECT * 
FROM staging.listing_5_6;
select * from data_warehouse.listings_agg;

MERGE INTO data_warehouse.listings_agg DL
USING staging.listing_7 SL
ON (DL.id = SL.id and DL.month = SL.month and DL.year = SL.year)
WHEN NOT MATCHED THEN
INSERT (
    id,
    listing_url,
    scrape_id,
    last_scraped,
    name,
    description,
    neighbourhood_overview,
    picture_url,
    host_id,
    host_url,
    host_name,
    host_since,
    host_location,
    host_about,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_thumbnail_url,
    host_picture_url,
    host_neighbourhood,
    host_listings_count,
    host_total_listings_count,
    host_verifications,
    host_has_profile_pic,
    host_identity_verified,
    neighbourhood,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    price,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    calendar_updated,
    has_availability,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    first_review,
    last_review,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    license,
    instant_bookable,
    calculated_host_listings_count,
    calculated_host_listings_count_entire_homes,
    calculated_host_listings_count_private_rooms,
    calculated_host_listings_count_shared_rooms,
    month,
    year
) VALUES (
    id,
    listing_url,
    scrape_id,
    last_scraped,
    name,
    description,
    neighbourhood_overview,
    picture_url,
    host_id,
    host_url,
    host_name,
    host_since,
    host_location,
    host_about,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_thumbnail_url,
    host_picture_url,
    host_neighbourhood,
    host_listings_count,
    host_total_listings_count,
    host_verifications,
    host_has_profile_pic,
    host_identity_verified,
    neighbourhood,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    price,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    calendar_updated,
    has_availability,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    first_review,
    last_review,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    license,
    instant_bookable,
    calculated_host_listings_count,
    calculated_host_listings_count_entire_homes,
    calculated_host_listings_count_private_rooms,
    calculated_host_listings_count_shared_rooms,
    month,
    year
);

MERGE INTO data_warehouse.listings_agg DL
USING staging.listing_rest SL
ON (DL.id = SL.id and DL.month = SL.month and DL.year = SL.year)
WHEN NOT MATCHED THEN
INSERT (
    id,
    listing_url,
    scrape_id,
    last_scraped,
    name,
    description,
    neighbourhood_overview,
    picture_url,
    host_id,
    host_url,
    host_name,
    host_since,
    host_location,
    host_about,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_thumbnail_url,
    host_picture_url,
    host_neighbourhood,
    host_listings_count,
    host_total_listings_count,
    host_verifications,
    host_has_profile_pic,
    host_identity_verified,
    neighbourhood,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    price,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    calendar_updated,
    has_availability,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    first_review,
    last_review,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    license,
    instant_bookable,
    calculated_host_listings_count,
    calculated_host_listings_count_entire_homes,
    calculated_host_listings_count_private_rooms,
    calculated_host_listings_count_shared_rooms,
    month,
    year
) VALUES (
    id,
    listing_url,
    scrape_id,
    last_scraped,
    name,
    description,
    neighbourhood_overview,
    picture_url,
    host_id,
    host_url,
    host_name,
    host_since,
    host_location,
    host_about,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_thumbnail_url,
    host_picture_url,
    host_neighbourhood,
    host_listings_count,
    host_total_listings_count,
    host_verifications,
    host_has_profile_pic,
    host_identity_verified,
    neighbourhood,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bathrooms,
    price,
    minimum_minimum_nights,
    maximum_minimum_nights,
    minimum_maximum_nights,
    maximum_maximum_nights,
    minimum_nights_avg_ntm,
    maximum_nights_avg_ntm,
    calendar_updated,
    has_availability,
    availability_30,
    availability_60,
    availability_90,
    availability_365,
    calendar_last_scraped,
    number_of_reviews,
    number_of_reviews_ltm,
    first_review,
    last_review,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    license,
    instant_bookable,
    calculated_host_listings_count,
    calculated_host_listings_count_entire_homes,
    calculated_host_listings_count_private_rooms,
    calculated_host_listings_count_shared_rooms,
    month,
    year
);
"""

data_warehouse = f"""

CREATE OR REPLACE TABLE data_warehouse.listing_final as (
SELECT *
FROM data_warehouse.listings_agg
    FULL OUTER JOIN data_warehouse.census_agg
    on trim(lower(listings_agg.neighbourhood_cleansed)) = trim(lower(census_agg.lga_name))
);

"""

data_mart = f"""
CREATE OR REPLACE SCHEMA datamart;
CREATE OR REPLACE TABLE datamart.datamart1 as(
with active_rates as(
select 
    distinct(neighbourhood_cleansed) as neighbourhood, month, year,
    sum(case when has_availability = 't' then 1 else 0 end) as total_active,
    sum(case when has_availability <> 't' then 1 else 0 end) as total_inactive,
    count(id) as total_listings,
    min(try_cast(replace(trim(price,'$'), ',','') as numeric)) as min_price,
    median(try_cast(replace(trim(price,'$'), ',','') as numeric)) as med_price,
    avg(try_cast(replace(trim(price,'$'), ',','') as numeric)) as avg_price,
    max(try_cast(replace(trim(price,'$'), ',','') as numeric)) as max_price,
    count(distinct(host_id)) as no_distinct_host,
    sum(case when host_is_superhost = 't' then 1 else 0 end) as total_superhost,
    avg(try_cast((review_scores_rating) as numeric)) as avg_review_scores_rating,
    lag(total_active) over (partition by month, year order by total_active) as change_active_listening,
    lag(total_inactive) over (partition by month, year order by total_inactive) as change_inactive_listening,
    sum(30-(try_cast(availability_30 as numeric))) as num_stays,
    round(avg((30-(try_cast(availability_30 as numeric))) *try_cast(replace(trim(price,'$'), ',','') as numeric)),3) as est_revenue_per_active_listing
from data_warehouse.listing_final
where neighbourhood_cleansed is not null
group by 1,2,3
)
select 
    neighbourhood, month, year,
    round((total_active/total_listings)*100, 3) as active_listing_rates,
    min_price,med_price,avg_price,max_price,
    no_distinct_host,
    round((total_superhost/no_distinct_host)*100, 3) as superhost_rate,
    avg_review_scores_rating,
    round(div0(change_active_listening,total_active)*100,3) as percentage_change_active,
    round(div0(change_inactive_listening,total_inactive)*100,3) as percentage_change_inactive,
    num_stays,
    est_revenue_per_active_listing
from active_rates);

CREATE OR REPLACE TABLE datamart.datamart2 as (
with active_rates as(
select 
    distinct(property_type) as property_type, room_type, accommodates, month, year,
    sum(case when has_availability = 't' then 1 else 0 end) as total_active,
    sum(case when has_availability <> 't' then 1 else 0 end) as total_inactive,
    count(id) as total_listings,
    min(try_cast(replace(trim(price,'$'), ',','') as numeric)) as min_price,
    median(try_cast(replace(trim(price,'$'), ',','') as numeric)) as med_price,
    avg(try_cast(replace(trim(price,'$'), ',','') as numeric)) as avg_price,
    max(try_cast(replace(trim(price,'$'), ',','') as numeric)) as max_price,
    count(distinct(host_id)) as no_distinct_host,
    sum(case when host_is_superhost = 't' then 1 else 0 end) as total_superhost,
    avg(try_cast((review_scores_rating) as numeric)) as avg_review_scores_rating,
    lag(total_active) over (partition by month, year order by total_active) as change_active_listening,
    lag(total_inactive) over (partition by month, year order by total_inactive) as change_inactive_listening,
    sum(30-(try_cast(availability_30 as numeric))) as num_stays,
    round(avg((30-(try_cast(availability_30 as numeric)))*try_cast(replace(trim(price,'$'), ',','') as numeric)),3) as est_revenue_per_active_listing
from data_warehouse.listing_final
where neighbourhood_cleansed is not null
group by 1,2,3,4,5
)
select 
    property_type, room_type, accommodates, month, year,
    round((total_active/total_listings)*100, 3) as active_listing_rates,
    min_price,med_price,avg_price,max_price,
    no_distinct_host,
    round((total_superhost/no_distinct_host)*100, 3) as superhost_rate,
    avg_review_scores_rating,
    round(div0(change_active_listening,total_active)*100,3) as percentage_change_active,
    round(div0(change_inactive_listening,total_inactive)*100,3) as percentage_change_inactive,
    num_stays,
    est_revenue_per_active_listing
from active_rates);

CREATE OR REPLACE TABLE datamart.datamart3 as (
with cte as (
    select
        distinct host_neighbourhood as host_neighbourhood, month, year,
        count(distinct(host_id)) as tot_distinct_hosts,
        sum(30-(try_cast(availability_30 as numeric))) as num_stays,
        avg(try_cast(replace(trim(price,'$'), ',','') as numeric)) as price
    from data_warehouse.listing_final
    where host_neighbourhood is not null
    group by 1,2,3
)
select 
host_neighbourhood, month, year,
tot_distinct_hosts,
round(num_stays * price, 3) as est_revenue,
round((num_stays * price)/tot_distinct_hosts, 3) as est_revenue_per_host
from cte);
"""





##################################################
#
#     DAG operator setup
#
##################################################

create_raw = SnowflakeOperator(
    task_id='create_raw_task',
    sql=create_raw,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_census = SnowflakeOperator(
    task_id='refresh_census_task',
    sql=refresh_census,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

refresh_listing= SnowflakeOperator(
    task_id='refresh_listing_task',
    sql=refresh_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

merge_schema = SnowflakeOperator(
    task_id='merge_schema_task',
    sql=merge_schema,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

data_warehouse = SnowflakeOperator (
    task_id='data_warehouse_task',
    sql=data_warehouse,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

data_mart = SnowflakeOperator(
    task_id='data_mart_task',
    sql=data_mart,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

# make create_raw run upstream from refresh_census and refresh_listing
create_raw >> refresh_census
create_raw >> refresh_listing
refresh_census >> merge_schema
refresh_listing >> merge_schema
merge_schema >> data_warehouse
data_warehouse >> data_mart