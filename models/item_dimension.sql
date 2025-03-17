with item_master_source as(
    select * from {{ source('tpch', 'item_master') }}
),
item_category_source as(
    select * from {{ source('tpch', 'item_category') }}
),
item_subcategory_source as(
    select * from {{ source('tpch', 'item_subcategory') }}
),

item_dimension as(
    select

        {{ dbt_utils.surrogate_key(
            ['item_master_source.ITEM_CD']) }}
                as item_key,
        item_master_source.ITEM_CD,
        item_master_source.ITEM_NAME,
        item_master_source.ITEM_CATEGORY_CD,
        item_category_source.ITEM_CATEGORY_NAME,
        item_master_source.ITEM_SUBCATEGORY_CD,
        item_subcategory_source.ITEM_SUBCATEGORY_NAME,
        item_master_source.STANDARD_PRICE,

    from item_master_source
    left outer join item_category_source
    on item_master_source.ITEM_CATEGORY_CD = item_category_source.ITEM_CATEGORY_CD
    left outer join item_subcategory_source
    on item_category_source.ITEM_CATEGORY_CD = item_subcategory_source.ITEM_CATEGORY_CD
)

select * from item_dimension