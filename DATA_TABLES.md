# Data Tables and Alert Context

Last updated: 2026-05-26

## BigQuery Dataset
- Project: `sm-test-391201`
- Dataset: `supermetrics_data`

## Table: `master_overview`
Purpose: Campaign metadata and rolled-up performance context used for campaign state checks.

Columns:
- `JOBNUMBER` STRING
- `OURREF` STRING
- `CAMPAIGNNAME` STRING
- `LOCATIONTEXT` STRING
- `PROPERTYNAME` STRING
- `STARTDATE` DATE
- `ENDDATE` DATE
- `NUMUNITS` INT64
- `ACTUALPRICE` FLOAT64
- `OURCOST` FLOAT64
- `T_GOAL_TYPE_REPORTING_V2` STRING
- `ACCOUNTMANAGERNAME` STRING
- `ADVERTISERNAME` STRING
- `BOOKINGSTATUS` STRING
- `TRAFFICKERNAME` STRING
- `CAMPAIGNLEAD` STRING
- `DATE` DATE
- `DATASOURCE` STRING
- `CLICKS` INT64
- `IMPRESSIONS` FLOAT64
- `COST` FLOAT64
- `VIDEO_PLAYS` FLOAT64
- `VIDEO_COMPLETIONS` FLOAT64
- `SPOTS` INT64
- `LINK_CLICKS` INT64

Notes:
- Primary key in alert logic is `OURREF` (mapped to delivery `OUR_REF`).
- `ENDDATE` is used as canonical campaign end date for post-end delivery checks.

## Table: `BLEND_BLEND_5_1_2`
Purpose: Daily delivery/performance rows used for missing-reference and post-end activity checks.

Columns:
- `ACCOUNT` STRING
- `ADVERTISER_NAME` STRING
- `BUDGET_SEGMENT_NAME` STRING
- `CAMPAIGN` STRING
- `DATASOURCENAME` STRING
- `DATE` DATE
- `END_DATE` STRING
- `JOB_NUMBER` STRING
- `OUR_REF` STRING
- `START_DATE` STRING
- `CLICKS` INT64
- `COST` FLOAT64
- `CTR` FLOAT64
- `IMPRESSIONS` FLOAT64
- `LINK_CLICKS` INT64
- `MEASURABLE_IMPRESSIONS_ACTIVE_VIEW` INT64
- `SPOTS` INT64
- `VIDEO_COMPLETIONS` FLOAT64
- `VIDEO_PLAYS` FLOAT64
- `VIEWABLE_IMPRESSIONS_ACTIVE_VIEW` INT64

Notes:
- Delivery date column is typed as DATE and used for timeline logic.
- `OUR_REF` coverage snapshot from 2026-05-26:
  - Delivery rows with non-empty `OUR_REF`: 25,412
  - Rows mapping to `master_overview.OURREF`: 19,387
  - Unmapped: 6,025

## Alert Types (campaign_not_live_alert.py)

### 1) `NOT_LIVE`
Flags campaigns that should be live but show zero impressions.

Rules:
- Grouped by `OURREF` from `master_overview`
- `STARTDATE < CURRENT_DATE('Pacific/Auckland')`
- `ENDDATE IS NOT NULL`
- `ENDDATE >= CURRENT_DATE('Pacific/Auckland')`
- `BOOKINGSTATUS = 'Booked'` (case-insensitive)
- `PROPERTYNAME` contains `Programmatic` (case-insensitive)
- `PROPERTYNAME` does not contain `Adserving` (case-insensitive)
- `MAX(IMPRESSIONS) = 0`

### 2) `MISSING_OUR_REF`
Flags delivery rows that have activity but no `OUR_REF`.

Rules:
- Source: `BLEND_BLEND_5_1_2`
- `TRIM(COALESCE(OUR_REF, '')) = ''`
- Exclude `ADVERTISER_NAME` containing `client card`
- Grouped by datasource/account/advertiser/campaign/job number
- `SUM(IMPRESSIONS) > 0`

### 3) `ENDED_BUT_IMPRESSIONS`
Flags campaigns that have ended but still receive impressions.

Rules (confirmed 2026-05-26):
- Source join: `master_overview` (`OURREF`) to delivery `BLEND_BLEND_5_1_2` (`OUR_REF`)
- Ended condition: `ENDDATE < CURRENT_DATE('Pacific/Auckland')`
- Post-end activity window: all delivery dates after `ENDDATE`
- Threshold: `SUM(IMPRESSIONS after end date) > 10`
- Same campaign filters as `NOT_LIVE`:
  - `BOOKINGSTATUS = 'Booked'`
  - property contains `Programmatic`
  - property excludes `Adserving`

### 4) `STOPPED_IMPRESSIONS`
Flags campaigns that are still active but appear to have stopped delivering.

Rules (updated 2026-08-17):
- Source join: `master_overview` (`OURREF`) to delivery `BLEND_BLEND_5_1_2` (`OUR_REF`)
- Latest data anchor: `MAX(BLEND_BLEND_5_1_2.DATE)` rather than system date
- Campaign must still be active:
  - `STARTDATE < latest delivery date`
  - `ENDDATE IS NOT NULL`
  - `ENDDATE >= latest delivery date`
- Same campaign filters as `NOT_LIVE`:
  - `BOOKINGSTATUS = 'Booked'`
  - property contains `Programmatic`
  - property excludes `Adserving`
- Prior activity threshold:
  - campaign must have previously delivered more than `200` impressions before the latest delivery date
- Stopped condition:
  - `0 impressions` on the latest delivery date
  - `0 impressions` on the previous delivery date
- Output fields for stopped-impressions alerts:
  - `LATEST_DELIVERY_DATE`: global freshest `DATE` present in `BLEND_BLEND_5_1_2`
  - `LAST_SEEN_DELIVERY_DATE`: last delivery `DATE` seen for that specific campaign
  - `LAST_NONZERO_IMPRESSIONS_DATE`: last `DATE` where that campaign had impressions greater than zero
  - `HISTORICAL_IMPRESSIONS_BEFORE_LATEST_DAY`: campaign impressions accumulated before the latest delivery date

Data recency check on 2026-06-02:
- `BLEND_BLEND_5_1_2` latest `DATE` = `2026-06-01`
- `master_overview` latest `DATE` = `2026-06-01`

## Snooze and Dismiss Model
- Shared BigQuery control table: `supermetrics_data.snoozes`
- Suppression key: `alert_type + alert_key`
- Statuses used: `ACTIVE`, `DISMISSED`, `UNSNOOZED`
- Active snoozes suppress until `snooze_end_date` (NZ date comparison)
- Dismissed alerts remain suppressed until manually unsnoozed/reopened in dashboard

## Snapshot and Dashboard
- Snapshot table: `supermetrics_data.live_alert_snapshots`
- Each email run stores row-level snapshot data keyed by `run_id`
- Email includes signed dashboard link using `mode=live_alerts&user&run_id&exp&sig`

## QA Table: `qa_video_on_trademe`
Purpose: Current rolling 7-day DV360 TradeMe video QA snapshot, loaded from the daily Gmail CSV attachment.

Load behavior:
- Source Gmail subject contains `TradeMe On Video - Last 7 Days`
- Fully overwritten on each successful daily ingestion
- Scheduled by `.github/workflows/qa_video_on_trademe.yml`
- Dashboard endpoint `/api/qa/video-on-trademe` reads this table instead of Gmail

Columns:
- `row_number` INT64
- `campaign` STRING
- `last_7_day_impressions` INT64
- `source_subject` STRING
- `source_message_id` STRING
- `source_attachment` STRING
- `email_received_at` TIMESTAMP
- `report_time` STRING
- `date_range` STRING
- `group_by` STRING
- `loaded_at` TIMESTAMP
