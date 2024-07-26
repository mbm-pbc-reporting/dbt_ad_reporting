{% if var('apple_search_ads__using_search_terms', True) %}
    {% set include_list = ['amazon_ads', 'apple_search_ads', 'microsoft_ads'] %}
{% else %}
    {% set include_list = ['amazon_ads', 'microsoft_ads'] %}
{% endif %}

{% set enabled_packages = get_enabled_packages(include=include_list)%}
{{ config(enabled=is_enabled(enabled_packages),
    unique_key = ['source_relation','platform','date_day','search_query','search_match_type','keyword_id','ad_group_id','campaign_id','account_id'],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    }
    ) }}


with base as (

    select *
    from {{ ref('int_ad_reporting__search_report') }}
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
        keyword_id,
        keyword_text,
        search_query,
        search_match_type,
        sum(clicks) as clicks,
        sum(impressions) as impressions,
        sum(spend) as spend 

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='ad_reporting__search_passthrough_metrics', transform = 'sum') }}

    from base
    {{ dbt_utils.group_by(13) }}
)

select *
from aggregated
