CREATE OR REPLACE EXTERNAL TABLE `sipher-data-platform.sipher_staging.playtest_survey`
OPTIONS (
  uris = ['https://docs.google.com/spreadsheets/d/1hXCoAGbIo0JJHNTQIhU45pgkrkQbt76mRzi7fQmF_bk'],
  sheet_range = 'Filter',
  skip_leading_rows = 1,
  format = 'GOOGLE_SHEETS'
);
