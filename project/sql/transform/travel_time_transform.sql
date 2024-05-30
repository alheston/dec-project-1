{% set config = {
    "extract_type":"full",
    "source_table_name":"travel_time_raw"
} %}

select
 search_id,
 case when cast(location_id as VARCHAR) = '40.8871438,-74.0410865' then 'Hackensack'
 when cast(location_id as VARCHAR) = '40.7433066,-74.0323752' then 'Hoboken'
 when cast(location_id as VARCHAR) = '41.0534302,-73.5387341' then 'Stamford'
 end as starting_city,
 travel_time / 60 as travel_time_minutes,
 load_timestamp,
 load_id
from
 {{config["source_table_name"]}}
