# CustomerLab Real-Time Attribution Pipeline

## Overview
This project implements a modular dbt pipeline for GA4 event data, designed for real-time attribution analysis.  
It includes source freshness checks, staging transformations, session aggregation, and attribution modeling.

## Project Structure
customer_lab/
|- models/
    |-sources/
       |_ ga4.yml
    |- staging
         |- stg_events.sql
    |- intermediate/
          |- int_session.sql
    |-marts/
        |- fct_attribution.sql
|- tests/
     |- schema.yml
|- dbt_project.yml


## Pipeline Flow
1. **Source (`ga4.yml`)**  
   Defines GA4 events table with freshness checks.

2. **Staging (`stg_events.sql`)**  
   Cleans and flattens raw GA4 events.  
   Handles schema drift and incremental loads.

3. **Intermediate (`int_sessions.sql`)**  
   Aggregates events into sessions.  
   Captures session start/end and traffic source.

4. **Mart (`fct_attribution.sql`)**  
   Applies attribution logic (last non-direct touch).  
   Produces final attribution dataset for reporting.

## Validation
- `dbt test` ensures column integrity (not_null, unique).  
- `dbt build` materializes models successfully in BigQuery.  
- Source freshness validates data timeliness.

## Next Steps
- Enhance attribution logic with multi-touch rules.  
- Build Looker Studio dashboard for visualization.  
- Add streaming ingestion (Pub/Sub → BigQuery → dbt incremental).
