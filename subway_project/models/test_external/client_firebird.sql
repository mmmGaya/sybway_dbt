select
    id as pk
    , name as client_name
from {{
    source('firebird', 'client')
}}