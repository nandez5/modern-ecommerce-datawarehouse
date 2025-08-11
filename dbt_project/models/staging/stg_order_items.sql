{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('raw_ecommerce', 'order_items') }}
),

transformed as (
    select
        -- Primary identifiers
        order_item_id,
        order_id,
        product_id,
        
        -- Quantity and pricing
        quantity,
        unit_price,
        line_total,
        cost_per_unit,
        line_cost,
        
        -- Calculate derived financial metrics
        line_total - line_cost as line_profit,
        round((line_total - line_cost) / line_total * 100, 2) as line_margin_percent,
        round(line_total / quantity, 2) as effective_unit_price,
        
        -- Metadata
        current_timestamp as _stg_loaded_at
        
    from source_data
),

final as (
    select 
        *,
        -- Business flags using calculated fields
        case when quantity > 1 then true else false end as is_multi_unit,
        case when line_total >= 200 then true else false end as is_high_value_line,
        case when unit_price != effective_unit_price then true else false end as has_line_discount
    from transformed
    where order_item_id is not null
      and order_id is not null
      and product_id is not null
      and quantity > 0
      and unit_price > 0
)

select * from final
