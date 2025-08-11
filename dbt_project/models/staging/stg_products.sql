{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('raw_ecommerce', 'products') }}
),

transformed as (
    select
        -- Primary identifiers
        product_id,
        upper(trim(sku)) as sku_standardized,
        
        -- Product information
        trim(product_name) as product_name,
        trim(brand) as brand,
        
        -- Category hierarchy
        trim(category_l1) as category_primary,
        trim(category_l2) as category_secondary,
        category_l1 || ' > ' || category_l2 as category_path,
        
        -- Pricing information
        retail_price,
        cost,
        retail_price - cost as gross_profit,
        round(margin_percent, 2) as margin_percent,
        
        -- Price classification
        case 
            when retail_price < 25 then 'Budget'
            when retail_price < 100 then 'Standard'
            when retail_price < 500 then 'Premium'
            else 'Luxury'
        end as price_tier,
        
        -- Physical attributes
        weight_kg,
        dimensions_cm,
        color,
        size,
        
        -- Inventory management
        stock_quantity,
        reorder_point,
        trim(supplier) as supplier,
        lifecycle_stage,
        is_active,
        is_featured,
        
        -- Performance metrics
        avg_rating,
        total_reviews,
        total_sales,
        
        -- Temporal information
        created_at::timestamp as product_created_at,
        
        -- Metadata
        current_timestamp as _stg_loaded_at
        
    from source_data
),

final as (
    select 
        *,
        -- Parse dimensions into separate fields
        case 
            when dimensions_cm is not null and position('x' in dimensions_cm) > 0
            then cast(split_part(dimensions_cm, 'x', 1) as numeric)
            else null 
        end as length_cm,
        
        case 
            when dimensions_cm is not null and position('x' in dimensions_cm) > 0
            then cast(split_part(dimensions_cm, 'x', 2) as numeric)
            else null 
        end as width_cm,
        
        case 
            when dimensions_cm is not null and position('x' in dimensions_cm) > 0
            then cast(split_part(dimensions_cm, 'x', 3) as numeric)
            else null 
        end as height_cm
    from transformed
    where product_id is not null 
      and retail_price is not null
      and retail_price > 0
)

select * from final
