--##################################################################################################
-- 
-- Part 1: Part 1: Download the datasets
-- 
--##################################################################################################

-- airflow

--##################################################################################################
-- 
-- Part 2: Design and populate a data warehouse
-- 
--##################################################################################################


CREATE SCHEMA staging;

CREATE OR REPLACE TABLE staging.staging_censusG01 as
SELECT
    value:c1::varchar as LGA_CODE_2016,
    value:c2::int as Tot_P_M,
    value:c100::int as High_yr_schl_comp_Yr_8_belw_P,
    value:c101::int as High_yr_schl_comp_D_n_g_sch_M,
    value:c102::int as High_yr_schl_comp_D_n_g_sch_F,
    value:c103::int as High_yr_schl_comp_D_n_g_sch_P,
    value:c104::int as Count_psns_occ_priv_dwgs_M,
    value:c105::int as Count_psns_occ_priv_dwgs_F,
    value:c106::int as Count_psns_occ_priv_dwgs_P,
    value:c107::int as Count_Persons_other_dwgs_M,
    value:c108::int as Count_Persons_other_dwgs_F,
    value:c109::int as Count_Persons_other_dwgs_P,
    value:c11::int as Age_15_19_yr_M,
    value:c12::int as Age_15_19_yr_F,
    value:c13::int as Age_15_19_yr_P,
    value:c14::int as Age_20_24_yr_M,
    value:c15::int as Age_20_24_yr_F,
    value:c16::int as Age_20_24_yr_P,
    value:c17::int as Age_25_34_yr_M,
    value:c18::int as Age_25_34_yr_F,
    value:c19::int as Age_25_34_yr_P,
    value:c20::int as Age_35_44_yr_M,
    value:c21::int as Age_35_44_yr_F,
    value:c22::int as Age_35_44_yr_P,
    value:c23::int as Age_45_54_yr_M, 
    value:c24::int as Age_45_54_yr_F, 
    value:c25::int as Age_45_54_yr_P, 
    value:c26::int as Age_55_64_yr_M,
    value:c27::int as Age_55_64_yr_F,
    value:c28::int as Age_55_64_yr_P,
    value:c29::int as Age_65_74_yr_M,
    value:c3::int as Tot_P_F,
    value:c30::int as Age_65_74_yr_F,
    value:c31::int as Age_65_74_yr_P,
    value:c32::int as Age_75_84_yr_M,
    value:c33::int as Age_75_84_yr_F,
    value:c34::int as Age_75_84_yr_P,
    value:c35::int as Age_85ov_M,
    value:c36::int as Age_85ov_F,
    value:c37::int as Age_85ov_P,
    value:c38::int as Counted_Census_Night_home_M,
    value:c39::int as Counted_Census_Night_home_F,
    value:c40::int as Counted_Census_Night_home_P,
    value:c41::int as Count_Census_Nt_Ewhere_Aust_M,
    value:c42::int as Count_Census_Nt_Ewhere_Aust_F,
    value:c43::int as Count_Census_Nt_Ewhere_Aust_P,
    value:c44::int as Indigenous_psns_Aboriginal_M,
    value:c45::int as Indigenous_psns_Aboriginal_F,
    value:c46::int as Indigenous_psns_Aboriginal_P,
    value:c47::int as Indig_psns_Torres_Strait_Is_M,
    value:c48::int as Indig_psns_Torres_Strait_Is_F,
    value:c49::int as Indig_psns_Torres_Strait_Is_P,
    value:c5::int as Age_0_4_yr_M,
    value:c50::int as Indig_Bth_Abor_Torres_St_Is_M,
    value:c51::int as Indig_Bth_Abor_Torres_St_Is_F,
    value:c52::int as Indig_Bth_Abor_Torres_St_Is_P,
    value:c53::int as Indigenous_P_Tot_M,
    value:c54::int as Indigenous_P_Tot_F,
    value:c55::int as Indigenous_P_Tot_P,
    value:c56::int as Birthplace_Australia_M,
    value:c57::int as Birthplace_Australia_F,
    value:c58::int as Birthplace_Australia_P,
    value:c59::int as Birthplace_Elsewhere_M,
    value:c6::int as Age_0_4_yr_F,
    value:c60::int as Birthplace_Elsewhere_F,
    value:c61::int as Birthplace_Elsewhere_P,
    value:c62::int as Lang_spoken_home_Eng_only_M,
    value:c63::int as Lang_spoken_home_Eng_only_F,
    value:c64::int as Lang_spoken_home_Eng_only_P,
    value:c65::int as Lang_spoken_home_Oth_Lang_M,
    value:c66::int as Lang_spoken_home_Oth_Lang_F,
    value:c67::int as Lang_spoken_home_Oth_Lang_P,
    value:c68::int as Australian_citizen_M,
    value:c69::int as Australian_citizen_F,
    value:c7::int as Age_0_4_yr_P,
    value:c70::int as Australian_citizen_P,
    value:c71::int as Age_psns_att_educ_inst_0_4_M,
    value:c72::int as Age_psns_att_educ_inst_0_4_F,
    value:c73::int as Age_psns_att_educ_inst_0_4_P,
    value:c74::int as Age_psns_att_educ_inst_5_14_M,
    value:c75::int as Age_psns_att_educ_inst_5_14_F,
    value:c76::int as Age_psns_att_educ_inst_5_14_P,
    value:c77::int as Age_psns_att_edu_inst_15_19_M,
    value:c78::int as Age_psns_att_edu_inst_15_19_F,
    value:c79::int as Age_psns_att_edu_inst_15_19_P,
    value:c8::int as Age_5_14_yr_M,
    value:c80::int as Age_psns_att_edu_inst_20_24_M,
    value:c81::int as Age_psns_att_edu_inst_20_24_F,
    value:c82::int as Age_psns_att_edu_inst_20_24_P,
    value:c83::int as Age_psns_att_edu_inst_25_ov_M,
    value:c84::int as Age_psns_att_edu_inst_25_ov_F,
    value:c85::int as Age_psns_att_edu_inst_25_ov_P,
    value:c86::int as High_yr_schl_comp_Yr_12_eq_M,
    value:c87::int as High_yr_schl_comp_Yr_12_eq_F,
    value:c88::int as High_yr_schl_comp_Yr_12_eq_P,
    value:c89::int as High_yr_schl_comp_Yr_11_eq_M,
    value:c9::int as Age_5_14_yr_F,
    value:c10::int as Age_5_14_yr_P,
    value:c90::int as High_yr_schl_comp_Yr_11_eq_F,
    value:c91::int as High_yr_schl_comp_Yr_11_eq_P,
    value:c92::int as High_yr_schl_comp_Yr_10_eq_M,
    value:c93::int as High_yr_schl_comp_Yr_10_eq_F,
    value:c94::int as High_yr_schl_comp_Yr_10_eq_P,
    value:c95::int as High_yr_schl_comp_Yr_9_eq_M,
    value:c96::int as High_yr_schl_comp_Yr_9_eq_F,
    value:c97::int as High_yr_schl_comp_Yr_9_eq_P,
    value:c98::int as High_yr_schl_comp_Yr_8_belw_M,
    value:c99::int as High_yr_schl_comp_Yr_8_belw_F
FROM raw.censusG01;

CREATE OR REPLACE TABLE staging.staging_censusG02 as
SELECT
    value:c1::varchar as LGA_CODE_2016,
    value:c2::int as Median_age_persons,
    value:c3::int as Median_mortgage_repay_monthly,
    value:c4::int as Median_tot_prsnl_inc_weekly,
    value:c5::int as Median_rent_weekly, 
    value:c6::int as Median_tot_fam_inc_weekly,
    value:c7::int as Average_num_psns_per_bedroom,
    value:c8::int as Median_tot_hhd_inc_weekly,
    value:c9::int as Average_household_size
FROM raw.censusg02;

CREATE OR REPLACE TABLE staging.staging_join_census as
SELECT 
    CONCAT('LGA', value:c1::varchar) as LGA_CODE,
    split_part(value:c2::varchar, ' (', 1) as LGA_NAME
FROM raw.join_census;

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
select * from staging.listing_7;

select * from staging.listing_5_6;

CREATE OR REPLACE VIEW data_warehouse.census as 
SELECT *
FROM staging.staging_censusg01 a 
INNER JOIN staging.staging_censusg02 b using (LGA_CODE_2016);

select * from data_warehouse.census;
select * from staging.staging_join_census;

CREATE OR REPLACE TABLE data_warehouse.census_agg as 
SELECT *
FROM data_warehouse.census a
INNER JOIN staging.staging_join_census b 
ON a.lga_code_2016 = b.lga_code;

select * from data_warehouse.census_agg;

CREATE OR REPLACE TABLE data_warehouse.listings_agg AS
SELECT * 
FROM staging.listing_5_6;
select * from data_warehouse.listings_agg;

-- Slowly Changing Dimension Type 1 handling
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

-- star schema with listings and census joined
CREATE OR REPLACE TABLE data_warehouse.listing_final as (
SELECT *
FROM data_warehouse.listings_agg
    FULL OUTER JOIN data_warehouse.census_agg
    on trim(lower(listings_agg.neighbourhood_cleansed)) = trim(lower(census_agg.lga_name))
);



--##################################################################################################
-- 
-- Part 3: Design and populate a data mart

--##################################################################################################

-- airflow









