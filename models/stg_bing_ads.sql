{{ config(enabled=var('ad_reporting__bing_ads_enabled')) }}

with base as (

    select *
    from {{ ref('bing_ads__ad_adapter')}}

), fields as (

    select
        'Bing Ads' as platform,
        date_day,
        account_name,
        account_id,
        campaign_name,
        campaign_id,
        ad_group_name,
        ad_group_id,
        base_url,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        clicks,
        impressions,
        spend
    from base


)

select *
from fields