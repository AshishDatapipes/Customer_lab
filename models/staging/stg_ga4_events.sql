{{
    config(
        materialized="incremental",
        unique_key="user_pseudo_id",
        incremental_strategy="merge",
    )
}}

select
    event_date,
    timestamp_micros(event_timestamp) as event_ts,
    user_pseudo_id,
    event_name,
    event_bundle_sequence_id,
    event_previous_timestamp,

    -- traffic source
    traffic_source.source as ts_source,
    traffic_source.medium as ts_medium,
    traffic_source.name as ts_campaign,

    -- page context
    (
        select ep.value.string_value
        from unnest(event_params) ep
        where ep.key = 'page_location'
        limit 1
    ) as page_location,
    (
        select ep.value.string_value
        from unnest(event_params) ep
        where ep.key = 'page_referrer'
        limit 1
    ) as page_referrer,

    -- device & geo
    device.category as device_category,
    geo.country as geo_country,
    geo.city as geo_city,

    -- ecommerce
    ecommerce.transaction_id as transaction_id

from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
