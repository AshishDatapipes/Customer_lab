{{ config(materialized='table') }}

select
    user_pseudo_id,
    session_date,
    events[OFFSET(array_length(events)-1)].ts_source   as last_click_source,
    events[OFFSET(array_length(events)-1)].ts_medium   as last_click_medium,
    events[OFFSET(array_length(events)-1)].ts_campaign as last_click_campaign
from {{ ref('int_sessions') }}
