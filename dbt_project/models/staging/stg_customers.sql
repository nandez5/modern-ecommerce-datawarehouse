{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('raw_ecommerce', 'customers') }}
),

transformed as (
    select
        -- Primary key
        customer_id,
        
        -- Personal information
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        lower(trim(email)) as email,
        
        -- Extract email domain for analysis
        split_part(lower(trim(email)), '@', 2) as email_domain,
        
        -- Demographics
        birth_date,
        case
            when lower(trim(gender)) in ('m', 'male') then 'Male'
            when lower(trim(gender)) in ('f', 'female') then 'Female'
            when lower(trim(gender)) in ('o', 'other') then 'Other'
            else 'Unknown'
        end as gender_standardized,
        
        -- Address information
        trim(address_line1) as address_line1,
        trim(city) as city,
        state,
        trim(postal_code) as postal_code,
        trim(country) as country,
        
        -- Business attributes
        customer_segment,
        acquisition_channel,
        lifetime_value,
        
        -- Status flags
        is_active,
        email_subscribed,
        preferred_contact,
        credit_score_range,
        
        -- Temporal fields
        created_at::timestamp as customer_created_at,
        updated_at::timestamp as customer_updated_at,
        last_order_date::date as last_order_date,
        
        -- Metadata
        current_timestamp as _stg_loaded_at
        
    from source_data
),

final as (
    select 
        *,
        -- Add derived flags after main transformation
        case when email_domain in ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com') then true else false end as uses_personal_email,
        case when customer_segment = 'VIP' then true else false end as is_vip
    from transformed
    where customer_id is not null
)

select * from final
