{{ config(materialized="table") }}

select
    user_pseudo_id,
    session_date,
    events[offset(0)].ts_source as first_click_source,
    events[offset(0)].ts_medium as first_click_medium,
    events[offset(0)].ts_campaign as first_click_campaign
from {{ ref("int_sessions") }}
