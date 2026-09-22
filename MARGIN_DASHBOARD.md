# Margin Dashboard

Last updated: 2026-09-22

## Objective

Build a live margin dashboard at `OUR REF` level using:
- gross billable amount from `master_overview.ACTUALPRICE`
- nett billable amount from `B27F8_BUY_DATA.NETBILLABLEAMOUNT`
- actual nett spend from delivery `BLEND_BLEND_5_1_2.COST`
- campaign dates from `master_overview.STARTDATE` and `master_overview.ENDDATE`

Checked-in view definition:
- [sql/margin_dashboard_view.sql](/Users/ashwin.sundaram/Library/CloudStorage/Dropbox-AcquireOnline/Ashwin Sundaram/Ashwin/Cerebro/Cerebrus/acquire-asana-creator/sql/margin_dashboard_view.sql)

## Data Sources

Primary tables:
- `sm-test-391201.supermetrics_data.master_overview`
- `sm-test-391201.supermetrics_data.B27F8_BUY_DATA`
- `sm-test-391201.supermetrics_data.BLEND_BLEND_5_1_2`

Primary joins:
- `master_overview.OURREF + JOBNUMBER = B27F8_BUY_DATA.OURREF + JOBNUMBER`
- `master_overview.OURREF = BLEND_BLEND_5_1_2.OUR_REF`

Grain:
- one row per `OUR REF`

## Metric Definitions

### Gross Billable

Definition:
- `budget = ACTUALPRICE`

Interpretation:
- this is booked gross revenue for the line

### Nett Billable

Definition:
- `nett_billable = NETBILLABLEAMOUNT`

Interpretation:
- this is the billable amount used as the margin denominator/base

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

### Expected Nett Billable To Date

Definition:
- `expected_nett_billable_to_date = nett_billable * elapsed_days / total_days`

Interpretation:
- linear pacing of nett billable value over campaign days

### Margin Amount

Definition:
- `margin_amount = expected_nett_billable_to_date - actual_nett_spend`

Interpretation:
- positive means current actual spend is below expected pace
- negative means current actual spend is above expected pace

### Margin Percent

Definition:
- `margin_pct = 1 - (actual_nett_spend / expected_nett_billable_to_date)`
- equivalent: `margin_amount / expected_nett_billable_to_date`

Guardrail:
- if `expected_nett_billable_to_date <= 0`, return `NULL`

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
- `MAX(ACTUALPRICE)` as gross billable
- `MAX(NETBILLABLEAMOUNT)` as nett billable
- `MIN(STARTDATE)` as start date
- `MAX(ENDDATE)` as end date

### Business Meaning

This is a pacing margin, not final realized campaign margin.

It compares:
- expected nett billable pace
against
- actual nett spend to date

If the business later wants true realized margin, the formula may need to change.

## Next Improvements

- add pacing status bands such as `on_track`, `over_pacing`, `under_pacing`
- decide how to treat unmapped delivery rows
- decide whether dashboard should show lines that have budget but zero delivery rows
- consider materializing a clean margin view in BigQuery for dashboard performance
