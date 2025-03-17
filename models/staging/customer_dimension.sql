with customer_source as(
    select * from {{ source('tpch', 'customer_master') }}
),
customer_dimension as(
    select 

        {{ dbt_utils.surrogate_key(
            ['CUSTOMER_ID']) }}
                as customer_key,
        CUSTOMER_ID,
        GENDER,
        BIRTHDAY,
        CUSTOMER_CLASS

    from customer_source
)

select * from customer_dimension
