{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('raw_ecommerce', 'orders') }}
),

transformed as (
    select
        -- Primary identifiers
        order_id,
        customer_id,
        
        -- Order timing
        order_date,
        created_at::timestamp as order_created_at,
        updated_at::timestamp as order_updated_at,
        
        -- Extract time components for analysis
        extract(year from order_date) as order_year,
        extract(month from order_date) as order_month,
        extract(dow from order_date) as order_day_of_week,
        extract(quarter from order_date) as order_quarter,
        
        -- Date classifications
        case 
            when extract(dow from order_date) in (0, 6) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        -- Order status standardization
        case 
            when lower(trim(order_status)) = 'pending' then 'Pending'
            when lower(trim(order_status)) = 'processing' then 'Processing'
            when lower(trim(order_status)) = 'shipped' then 'Shipped'
            when lower(trim(order_status)) = 'delivered' then 'Delivered'
            when lower(trim(order_status)) = 'cancelled' then 'Cancelled'
            when lower(trim(order_status)) = 'returned' then 'Returned'
            else 'Unknown'
        end as order_status_standardized,
        
        -- Payment method standardization
        case 
            when lower(trim(payment_method)) = 'credit_card' then 'Credit Card'
            when lower(trim(payment_method)) = 'debit_card' then 'Debit Card'
            when lower(trim(payment_method)) = 'paypal' then 'PayPal'
            when lower(trim(payment_method)) = 'apple_pay' then 'Apple Pay'
            when lower(trim(payment_method)) = 'google_pay' then 'Google Pay'
            when lower(trim(payment_method)) = 'bank_transfer' then 'Bank Transfer'
            else 'Other'
        end as payment_method_standardized,
        
        -- Order financial components
        total_items,
        subtotal,
        discount_amount,
        tax_amount,
        shipping_cost,
        total_amount,
        currency,
        
        -- Customer acquisition and channel data
        acquisition_channel,
        device_type,
        is_first_order,
        
        -- Metadata
        current_timestamp as _stg_loaded_at
        
    from source_data
),

final as (
    select 
        *,
        -- Business flags
        case when discount_amount > 0 then true else false end as has_discount,
        case when shipping_cost = 0 then true else false end as free_shipping,
        case when payment_method_standardized in ('Credit Card', 'Debit Card') then true else false end as paid_with_card,
        case when order_status_standardized in ('Delivered') then true else false end as is_completed
    from transformed
    where order_id is not null
      and customer_id is not null
      and order_date is not null
      and total_amount > 0
)

select * from final
