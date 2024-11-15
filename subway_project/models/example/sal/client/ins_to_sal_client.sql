{{ ins_sal_macros( 'tbl_union_client_sat', "CLIENT", "client_rk", ("name_desc", "birthday_dt")) }}

--depends on {{ ref('tbl_union_client_sat') }}