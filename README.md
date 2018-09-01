# clickhouse
Library for easy use of Clickhouse db.
It uses Pandas and pandahouse library to process and display data.

How to use:
from core import *

select (
query : SQL query
output : 'table' - pandas DataFrame; 'dict' - list of dicts; 'csv' - a .csv file
file : name of a file to write into. Only used by 'csv' output
)

insert (
data : data to load into clickhouse. Should be either a list of dicts OR DataFrame with 1 column containing json'ed data. Column name does not matter, it will be 'json' always
table : target table in your db
event : column 'event' in table. For example 'login' or 'payment'
)

delete (
table : table to delete data from
event : type of event to delete. For example delete all 'login' events
)

replace (
data : data to insert in place of removed data
table : target table in your db
event : column 'event' in your table, that will be replaced with new data
)

Library supports both single and clustered servers.
In case of single: write server url into config (cluster_connector)
In case of multiple nodes: write cluster url into cluster_connector and each node's url into nodes_connectors


Basic table structure:
    CREATE TABLE (
        insert_time DateTime,
        event String,
        json String
    )

All other columns can be extracted from json automatically using ex.:
column_name String DEFAULT visitParamExtractString(json, 'column_name')