{% set enabled_packages = get_enabled_packages() %}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with base as (

    select *
    from {{ ref('int_ad_reporting__account_report') }}
),

aggregated as (
    
    select
        source_relation,
        date_day,
        platform,
        account_id,
        account_name,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__account_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(5) }}
)
,
all_data as(
select *
from aggregated

union all

SELECT 
source_relation
,date_day
,platform
,account_id
,account_name
,clicks
,impressions
,spend
,conversions    
from {{ ref('ttd_ads__custom_account_report') }}
union all    
    
SELECT 
source_relation
,date_day
,'youtube' as platform
,cast(account_id as string)
,account_name
,sum(clicks) as clicks
,sum(impressions) as impressions
,sum(spend) as spend
,sum(conversions) as conversions    
from 
--`pbc-reporting-prod`.`mother_ny_pbc_youtube_summary_prod`.`youtube_ads__custom_ad_summary_report` 
    {{ref('youtube_ads__custom_ad_summary_report')}}
group by 1,2,3,4,5

union all

SELECT 
source_relation
,date_day
,'performance_max' as platform
,cast(account_id as string)
,account_name
,sum(clicks) as clicks
,sum(impressions) as impressions
,sum(spend) as spend
,sum(conversions) as conversions    
from   
    {{ref('performance_max_ads__custom_ad_summary_report')}}
--`pbc-reporting-prod`.`mother_ny_pbc_performance_max_summary_prod`.`performance_max_ads__custom_ad_summary_report` 
group by 1,2,3,4,5
    
    )

select *
from all_data
