{{
    config(
        materialized="incremental",
        unique_key="event_id",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
    )
}}

with
    base as (
        select
            cast(event_timestamp as timestamp) as event_ts,
            event_date,
            event_name,
            user_pseudo_id,
            event_id,
            traffic_source.source as source,
            traffic_source.medium as medium,
            traffic_source.campaign as campaign,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'page_location'
            ) as page_location,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'session_id'
            ) as session_id
        from {{ source("ga4", "events") }}
        {% if is_incremental() %}
            where
                cast(event_timestamp as timestamp) > (
                    select coalesce(max(event_ts), timestamp('1970-01-01'))
                    from {{ this }}
                )
        {% endif %}
    )
select *
from base
;
