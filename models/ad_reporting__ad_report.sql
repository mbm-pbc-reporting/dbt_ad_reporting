{% set enabled_packages = get_enabled_packages() %}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','ad_id','ad_group_id','campaign_id','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
    ) }}

with base as (

    select *
    from {{ ref('int_ad_reporting__ad_report') }}
),

aggregated as (
    
    select
        source_relation,
        date_day,
        platform,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        ad_group_id,
        ad_group_name,
        ad_id,
        ad_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 
        
        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__ad_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(11) }}
),
 youtube_cte AS (
  SELECT
    *
  FROM
    {{ref('google_ads__ad_report')}}
   -- `pbc-reporting-dev.mother_ny_pbc_googleads_summary_dev.google_ads__ad_report`
  WHERE lower(ad_type) like '%video%' )

,all_data as(
select *
from aggregated

union all

SELECT 
source_relation
,date_day
,platform
,account_id
,account_name
,campaign_id
,campaign_name
,ad_group_id
,ad_group_name
,ad_id
,ad_name
,clicks
,impressions
,spend
,conversions    
from 
{{ ref('ttd_ads__custom_ad_report') }} 

union all

  SELECT
    source_relation,
    date_day,
    'youtube' AS platform,
    cast(account_id as string),
    account_name,
    cast(campaign_id as string),
    campaign_name,
    cast(ad_group_id as string),
    ad_group_name,
    cast(ad_id as string),
    ad_name,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(spend) AS spend,
    SUM(conversions) AS conversions,
  FROM
    youtube_cte
  GROUP BY  1,2,3,4,5,6,7,8,9,10,11    
)
select *
from all_data
