with store_source as(
    select * from {{ source('tpch', 'store_master') }}
),
area_source as(
    select * from {{ source('tpch', 'area') }}
),

store_dimension as(
    select 

        {{ dbt_utils.surrogate_key(
            ['store_source.STORE_ID']) }}
                as store_key,
        store_source.STORE_ID,
        store_source.STORE_NAME,
        store_source.AREA_CD,
        area_source.AREA_NAME,
        store_source.DIRECT_FLG

    from store_source
    left outer join area_source
    on store_source.AREA_CD = area_source.AREA_CD
)

select * from store_dimension