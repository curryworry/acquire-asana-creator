CREATE OR REPLACE VIEW `sm-test-391201.supermetrics_data.margin_dashboard` AS
WITH latest_delivery AS (
  SELECT MAX(DATE) AS latest_delivery_date
  FROM `sm-test-391201.supermetrics_data.BLEND_BLEND_5_1_2`
),
line_items AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS our_ref,
    CAST(JOBNUMBER AS STRING) AS job_number,
    MAX(CAMPAIGNNAME) AS campaign_name,
    MAX(ADVERTISERNAME) AS advertiser_name,
    MAX(PROPERTYNAME) AS property_name,
    MAX(LOCATIONTEXT) AS location_text,
    MAX(ACCOUNTMANAGERNAME) AS account_manager_name,
    MAX(TRAFFICKERNAME) AS trafficker_name,
    MAX(CAMPAIGNLEAD) AS campaign_lead,
    MAX(BOOKINGSTATUS) AS booking_status,
    MAX(ACTUALPRICE) AS budget,
    MAX(OURCOST) AS booked_nett_cost,
    COALESCE(
      MIN(SAFE_CAST(STARTDATE AS DATE)),
      MIN(SAFE.PARSE_DATE('%Y-%m-%d', CAST(STARTDATE AS STRING))),
      MIN(SAFE.PARSE_DATE('%d/%m/%Y', CAST(STARTDATE AS STRING))),
      MIN(SAFE.PARSE_DATE('%m/%d/%Y', CAST(STARTDATE AS STRING)))
    ) AS start_date,
    COALESCE(
      MAX(SAFE_CAST(ENDDATE AS DATE)),
      MAX(SAFE.PARSE_DATE('%Y-%m-%d', CAST(ENDDATE AS STRING))),
      MAX(SAFE.PARSE_DATE('%d/%m/%Y', CAST(ENDDATE AS STRING))),
      MAX(SAFE.PARSE_DATE('%m/%d/%Y', CAST(ENDDATE AS STRING)))
    ) AS end_date
  FROM `sm-test-391201.supermetrics_data.master_overview`
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
  GROUP BY 1, 2
),
delivery AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS our_ref,
    SUM(COALESCE(COST, 0)) AS actual_nett_spend,
    SUM(COALESCE(IMPRESSIONS, 0)) AS total_impressions,
    SUM(COALESCE(CLICKS, 0)) AS total_clicks,
    MIN(DATE) AS first_delivery_date,
    MAX(DATE) AS last_delivery_date
  FROM `sm-test-391201.supermetrics_data.BLEND_BLEND_5_1_2`
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
  GROUP BY 1
),
base AS (
  SELECT
    l.our_ref,
    l.job_number,
    l.campaign_name,
    l.advertiser_name,
    l.property_name,
    l.location_text,
    l.account_manager_name,
    l.trafficker_name,
    l.campaign_lead,
    l.booking_status,
    l.budget,
    l.booked_nett_cost,
    l.start_date,
    l.end_date,
    ld.latest_delivery_date,
    LEAST(ld.latest_delivery_date, l.end_date) AS as_of_date,
    DATE_DIFF(l.end_date, l.start_date, DAY) + 1 AS total_days,
    GREATEST(
      0,
      LEAST(
        DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
        DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
      )
    ) AS elapsed_days,
    COALESCE(d.actual_nett_spend, 0) AS actual_nett_spend,
    COALESCE(d.total_impressions, 0) AS total_impressions,
    COALESCE(d.total_clicks, 0) AS total_clicks,
    d.first_delivery_date,
    d.last_delivery_date
  FROM line_items l
  CROSS JOIN latest_delivery ld
  LEFT JOIN delivery d
    ON d.our_ref = l.our_ref
  WHERE l.start_date IS NOT NULL
    AND l.end_date IS NOT NULL
    AND l.end_date >= l.start_date
)
SELECT
  our_ref,
  job_number,
  campaign_name,
  advertiser_name,
  property_name,
  location_text,
  account_manager_name,
  trafficker_name,
  campaign_lead,
  booking_status,
  budget,
  booked_nett_cost,
  start_date,
  end_date,
  latest_delivery_date,
  as_of_date,
  total_days,
  elapsed_days,
  SAFE_DIVIDE(elapsed_days, total_days) AS pacing_ratio,
  actual_nett_spend,
  total_impressions,
  total_clicks,
  first_delivery_date,
  last_delivery_date,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) AS expected_gross_spend_to_date,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) - actual_nett_spend AS margin_amount,
  CASE
    WHEN SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) > 0
    THEN 1 - SAFE_DIVIDE(actual_nett_spend, SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)))
    ELSE NULL
  END AS margin_pct,
  SAFE_DIVIDE(actual_nett_spend, budget) AS spend_vs_budget_ratio
FROM base;
