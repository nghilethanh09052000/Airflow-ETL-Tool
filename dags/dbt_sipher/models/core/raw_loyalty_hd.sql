{{ config(materialized='table') }}


with raw_loyalty_hd as (
    select * 
    ,LENGTH(wallet_address) as cnt_char_wallet_address
    from `sipher-data-platform.sipher_presentation.raw_loyalty_hd`
    -- where wallet_address is not null
    -- limit 1000
)

,final as (
    select * from raw_loyalty_hd
)

select * from final