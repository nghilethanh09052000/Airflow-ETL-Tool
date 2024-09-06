{{config(materialized='table')}}

select * from `sipher-data-platform.sipher_presentation.SIPHER_token_holders_overview`
where user_address not in ('0x000000000000000000000000000000000000dead')