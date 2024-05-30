{% set config = {
    "extract_type":"full",
    "source_table_name":"travel_time_raw"
} %}

select
 search_id,
 location_id,
 travel_time,
 load_timestamp,
 load_id
from
 {{config["source_table_name"]}}
