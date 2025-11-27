{{ config(materialized="table") }}

-- Step 1: Take events from staging
with
    base as (
        select user_pseudo_id, event_ts, event_name, ts_source, ts_medium, ts_campaign
        from {{ ref("stg_ga4_events") }}
    )

-- Step 2: Define sessions by user and day
select
    user_pseudo_id,
    date(event_ts) as session_date,
    min(event_ts) as session_start,
    max(event_ts) as session_end,
    array_agg(
        struct(event_ts, ts_source, ts_medium, ts_campaign, event_name)
        order by event_ts
    ) as events
from base
group by user_pseudo_id, date(event_ts)
