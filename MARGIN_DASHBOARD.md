# Margin Dashboard

Last updated: 2026-06-12

## Objective

Build a live margin dashboard at `OUR REF` level using:
- booked revenue budget from `master_overview.ACTUALPRICE`
- actual nett spend from delivery `BLEND_BLEND_5_1_2.COST`
- campaign dates from `master_overview.STARTDATE` and `master_overview.ENDDATE`

Checked-in view definition:
- [sql/margin_dashboard_view.sql](/Users/ashwin.sundaram/Library/CloudStorage/Dropbox-AcquireOnline/Ashwin Sundaram/Ashwin/Cerebro/Cerebrus/acquire-asana-creator/sql/margin_dashboard_view.sql)

## Data Sources

Primary tables:
- `sm-test-391201.supermetrics_data.master_overview`
- `sm-test-391201.supermetrics_data.BLEND_BLEND_5_1_2`

Primary join:
- `master_overview.OURREF = BLEND_BLEND_5_1_2.OUR_REF`

Grain:
- one row per `OUR REF`

## Metric Definitions

### Budget

Definition:
- `budget = ACTUALPRICE`

Interpretation:
- this is booked gross revenue for the line

### Actual Nett Spend

Definition:
- `actual_nett_spend = SUM(COST)` from delivery rows up to the latest available delivery date

Interpretation:
- this is actual media spend to date

### As-Of Date

Do not use system current date directly.

Definition:
- `latest_delivery_date = MAX(BLEND_BLEND_5_1_2.DATE)`
- `as_of_date = LEAST(latest_delivery_date, end_date)`

Reason:
- delivery data can lag behind calendar date
- using `CURRENT_DATE()` can overstate elapsed days and distort pacing

### Total Days

Definition:
- `total_days = DATE_DIFF(end_date, start_date, DAY) + 1`

Notes:
- inclusive of both start and end date
- if dates are invalid or missing, this should be treated carefully in SQL

### Elapsed Days

Definition:
- before campaign starts: `0`
- after campaign ends: `total_days`
- otherwise: `DATE_DIFF(as_of_date, start_date, DAY) + 1`

Equivalent clamp:
- `elapsed_days = GREATEST(0, LEAST(total_days, DATE_DIFF(as_of_date, start_date, DAY) + 1))`

### Expected Gross Spend To Date

Definition:
- `expected_gross_spend_to_date = budget * elapsed_days / total_days`

Interpretation:
- linear pacing of booked revenue over campaign days

### Margin Amount

Definition:
- `margin_amount = expected_gross_spend_to_date - actual_nett_spend`

Interpretation:
- positive means current actual spend is below expected pace
- negative means current actual spend is above expected pace

### Margin Percent

Definition:
- `margin_pct = 1 - (actual_nett_spend / expected_gross_spend_to_date)`

Guardrail:
- if `expected_gross_spend_to_date <= 0`, return `NULL`

Interpretation:
- `0%` means exactly on expected pace
- positive means under-spent versus expected pace
- negative means over-spent versus expected pace

## Important Caveats

### Unmapped Delivery Rows

Some delivery rows do not map to `master_overview.OURREF`.

Impact:
- those rows will be excluded from `OUR REF` level margin calculations unless a fallback matching rule is introduced

### Duplicate Daily Rows In `master_overview`

`master_overview` contains daily rows, not one static row per line.

Implication:
- budget and dates should be aggregated to stable line-level values before joining to delivery

Recommended line-level rollup:
- `MAX(ACTUALPRICE)` as budget
- `MIN(STARTDATE)` as start date
- `MAX(ENDDATE)` as end date

### Business Meaning

This is a pacing margin, not final realized campaign margin.

It compares:
- expected gross revenue pace
against
- actual nett spend to date

If the business later wants true realized margin, the formula may need to change.

## First-Pass Query

```sql
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
    MAX(ACTUALPRICE) AS budget,
    MIN(SAFE_CAST(STARTDATE AS DATE)) AS start_date,
    MAX(SAFE_CAST(ENDDATE AS DATE)) AS end_date
  FROM `sm-test-391201.supermetrics_data.master_overview`
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
  GROUP BY 1, 2
),
delivery AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS our_ref,
    SUM(COALESCE(COST, 0)) AS actual_nett_spend
  FROM `sm-test-391201.supermetrics_data.BLEND_BLEND_5_1_2`
  CROSS JOIN latest_delivery
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
    AND DATE <= latest_delivery_date
  GROUP BY 1
)
SELECT
  l.our_ref,
  l.job_number,
  l.campaign_name,
  l.advertiser_name,
  l.budget,
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
  SAFE_MULTIPLY(
    l.budget,
    SAFE_DIVIDE(
      GREATEST(
        0,
        LEAST(
          DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
          DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
        )
      ),
      DATE_DIFF(l.end_date, l.start_date, DAY) + 1
    )
  ) AS expected_gross_spend_to_date,
  SAFE_MULTIPLY(
    l.budget,
    SAFE_DIVIDE(
      GREATEST(
        0,
        LEAST(
          DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
          DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
        )
      ),
      DATE_DIFF(l.end_date, l.start_date, DAY) + 1
    )
  ) - COALESCE(d.actual_nett_spend, 0) AS margin_amount,
  CASE
    WHEN SAFE_MULTIPLY(
      l.budget,
      SAFE_DIVIDE(
        GREATEST(
          0,
          LEAST(
            DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
            DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
          )
        ),
        DATE_DIFF(l.end_date, l.start_date, DAY) + 1
      )
    ) > 0
    THEN 1 - SAFE_DIVIDE(
      COALESCE(d.actual_nett_spend, 0),
      SAFE_MULTIPLY(
        l.budget,
        SAFE_DIVIDE(
          GREATEST(
            0,
            LEAST(
              DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
              DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
            )
          ),
          DATE_DIFF(l.end_date, l.start_date, DAY) + 1
        )
      )
    )
    ELSE NULL
  END AS margin_pct
FROM line_items l
CROSS JOIN latest_delivery ld
LEFT JOIN delivery d
  ON d.our_ref = l.our_ref
WHERE l.start_date IS NOT NULL
  AND l.end_date IS NOT NULL
  AND l.end_date >= l.start_date
ORDER BY margin_amount ASC, l.our_ref;
```

## Next Improvements

- add pacing status bands such as `on_track`, `over_pacing`, `under_pacing`
- decide how to treat unmapped delivery rows
- decide whether dashboard should show lines that have budget but zero delivery rows
- consider materializing a clean margin view in BigQuery for dashboard performance
