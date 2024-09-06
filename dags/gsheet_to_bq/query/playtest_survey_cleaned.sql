CREATE TEMP FUNCTION SPLIT_TRIM(column STRING)
RETURNS ARRAY<STRING> AS (
  ARRAY(SELECT TRIM(e) FROM UNNEST(SPLIT(column, ',')) AS e)
);

CREATE OR REPLACE TABLE `{{ params.bq_project }}.sipher_presentation.playtest_survey`
PARTITION BY DATE(Timestamp)
AS

WITH
  testing_email_regexp AS
    (SELECT
      STRING_AGG(
        CASE
          WHEN type = "suffix" THEN "(" || text || ")" || "$"
          WHEN type = "prefix" THEN "^" || "(" || text || ")"
          ELSE "(" || text || ")"
        END,
        "|"
      )
    FROM `sipher-data-platform.sipher_staging.playtest_survey_testing_email_list`
    WHERE text IS NOT NULL)

SELECT * REPLACE(
  SPLIT_TRIM(Which_game_genres_do_you_like_to_play_the_most_) AS Which_game_genres_do_you_like_to_play_the_most_,
  SPLIT_TRIM(Please_input_1_3_of_your_favorite_game_titles_franchises) AS Please_input_1_3_of_your_favorite_game_titles_franchises,
  SPLIT_TRIM(Which_of_the_following_is_important_when_choosing_a_game_to_purchase_) AS Which_of_the_following_is_important_when_choosing_a_game_to_purchase_,
  SPLIT_TRIM(Token_earning_pathways) AS Token_earning_pathways,
  SPLIT_TRIM(Which_of_the_below_you_re_holding_) AS Which_of_the_below_you_re_holding_,
  SPLIT_TRIM(Which_NFTs_are_you_holding_) AS Which_NFTs_are_you_holding_,
  SPLIT_TRIM(Which_exchanges_do_you_you_the_most) AS Which_exchanges_do_you_you_the_most,
  SPLIT_TRIM(Which_NFT_marketplace_do_you_use_the_most_) AS Which_NFT_marketplace_do_you_use_the_most_,
  SPLIT_TRIM(How_did_you_find_us_) AS How_did_you_find_us_
)
FROM `sipher-data-platform.sipher_staging.playtest_survey`
WHERE NOT REGEXP_CONTAINS(Email_Address, (SELECT * FROM testing_email_regexp))