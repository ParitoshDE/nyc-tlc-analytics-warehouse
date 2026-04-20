# Dashboard Blueprint (Submission Ready)

## Goal
Build one Looker Studio dashboard on top of BigQuery dataset `nyc_tlc_prod` that answers:
- demand trend over time
- revenue trend over time
- hourly and weekday behavior
- zone-level and route-level performance
- payment-type performance

## Prerequisites
1. dbt models already built in `dataengkestra.nyc_tlc_prod`.
2. Run SQL from [scripts/dashboard_views.sql](../scripts/dashboard_views.sql) in BigQuery.
3. Open Looker Studio and connect to BigQuery project `dataengkestra`.

## Data Sources
Create 5 data sources from these views:
1. `dataengkestra.nyc_tlc_prod.dashboard_kpi_daily`
2. `dataengkestra.nyc_tlc_prod.dashboard_hourly_traffic`
3. `dataengkestra.nyc_tlc_prod.dashboard_payment_performance`
4. `dataengkestra.nyc_tlc_prod.dashboard_zone_performance`
5. `dataengkestra.nyc_tlc_prod.dashboard_route_performance`

## Page 1: Executive Overview
Add scorecards:
1. Total Trips: `SUM(total_trips)` from `dashboard_kpi_daily`
2. Total Revenue: `SUM(total_revenue)` from `dashboard_kpi_daily`
3. Avg Ticket: `AVG(avg_ticket)` from `dashboard_kpi_daily`
4. Avg Trip Duration (min): `AVG(avg_trip_duration_min)` from `dashboard_kpi_daily`

Add charts:
1. Time Series - Monthly Trips
   - Dimension: `pickup_date`
   - Metric: `total_trips`
   - Granularity: `Year Month`
2. Time Series - Monthly Revenue
   - Dimension: `pickup_date`
   - Metric: `total_revenue`
   - Granularity: `Year Month`

Add controls:
1. Date range control (default: custom `2023-01-01` to `2023-12-31`)
2. Ensure report time zone is set consistently (recommended: project default)

### Important Data Refresh Step (Looker Studio)
After SQL/view changes, run this before checking charts:
1. `Resource -> Manage added data sources`
2. Edit `dashboard_kpi_daily`
3. Click `Refresh fields`
4. Confirm `pickup_date` is of type `Date`
5. Return to report and re-open the chart

### Time Series Troubleshooting
If time series looks incorrect:
1. Check chart dimension = `pickup_date`
2. Check metric aggregation = `SUM(total_trips)` / `SUM(total_revenue)`
3. Check granularity = `Year Month`
4. Check page/report date range covers only 2023
5. Confirm data source is `dashboard_kpi_daily` (not `fct_event` directly)

## Page 2: Traffic Pattern
Charts using `dashboard_hourly_traffic`:
1. Heatmap (or pivot table)
   - Rows: `day_name`
   - Columns: `pickup_hour`
   - Metric: `SUM(total_trips)`
2. Bar chart - Revenue by Hour
   - Dimension: `pickup_hour`
   - Metric: `SUM(total_revenue)`
3. Bar chart - Avg Trip Duration by Hour
   - Dimension: `pickup_hour`
   - Metric: `AVG(avg_trip_duration_min)`

Controls:
1. Drop-down: `day_name`

## Page 3: Payment Performance
Charts using `dashboard_payment_performance`:
1. Bar chart - Trips by Payment Type
   - Dimension: `payment_type_name`
   - Metric: `total_trips`
2. Bar chart - Revenue by Payment Type
   - Dimension: `payment_type_name`
   - Metric: `total_revenue`
3. Table - Tip Ratio by Payment Type
   - Columns: `payment_type_name`, `avg_tip_amount`, `tip_to_fare_ratio`

## Page 4: Zone and Route Analysis
Charts:
1. Table from `dashboard_zone_performance`
   - Columns: `pulocation_id`, `total_trips`, `total_revenue`, `avg_total_amount`, `avg_trip_distance`
   - Sort: `total_revenue` desc
   - Filter: top 20 rows
2. Table from `dashboard_route_performance`
   - Columns: `pickup_date`, `pulocation_id`, `dolocation_id`, `total_trips`, `total_revenue`
   - Sort: `total_revenue` desc
3. Optional Geo map:
   - If zone lookup is added later, map by zone dimension and color by `total_revenue`

Controls:
1. Date range control
2. Numeric filter: `pulocation_id`
3. Numeric filter: `dolocation_id`

## Recommended Styling
1. Title: NYC TLC Analytics Dashboard
2. Theme: light background, high contrast
3. Number formatting:
   - Revenue: currency
   - Ratios: percent with 2 decimals
   - Duration and distance: 2 decimals

## Submission Checklist
1. Dashboard contains at least 4 pages.
2. Every chart uses curated views from `nyc_tlc_prod`.
3. At least one time series, one table, and one comparative bar chart included.
4. Date range filter works across pages.
5. Screenshot of each page added to submission.
6. Include report share link in your README submission notes.

### Shared Dashboard Link
https://datastudio.google.com/reporting/b836db6d-8fbd-4f56-87ef-887983634be8

## Optional Next Improvements
1. Add taxi zone name mapping table to replace location IDs with human-friendly labels.
2. Add anomaly alert scorecards (week-over-week drop in trips/revenue).
3. Add service-level KPIs by vendor and payment type.
