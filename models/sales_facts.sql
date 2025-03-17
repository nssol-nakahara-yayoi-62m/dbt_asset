with customer_dimension as (
    
    select * from {{ ref('customer_dimension') }}

),
item_dimension as (
    
    select * from {{ ref('item_dimension') }}

),
store_dimension as (
        
    select * from {{ ref('store_dimension') }}

),
time_dimension as (

    select * from {{ ref('time_dimension') }}

),
sales_detail as (

    select * from {{ source('tpch', 'sales_detail') }}

),
sales as (

    select * from {{ source('tpch', 'sales') }}

),

sales_facts as (

    select
        customer_dimension.customer_key,
        store_dimension.store_key,
        item_dimension.item_key,
--        time_dimension.time_key,
        sales_detail.SALES_QUANTITY,
        sales_detail.SALES_UNIT_PRICE,
        sales_detail.SALES_UNIT_PRICE*sales_detail.SALES_QUANTITY as SELL_UNIT_PRICE
        
    from

        sales
        left outer join sales_detail
        on sales.STORE_ID = sales_detail.STORE_ID
        and sales.REGISTER_NO = sales_detail.REGISTER_NO
        and sales.RECEIPT_NO = sales_detail.RECEIPT_NO

        left outer join customer_dimension
        on sales.customer_ID = customer_dimension.customer_ID

        left outer join store_dimension
        on sales.STORE_ID = store_dimension.STORE_ID

        left outer join item_dimension
        on sales_detail.ITEM_CD = item_dimension.ITEM_CD

--        left outer join time_dimension
--        on sales.SALES_DATE = time_dimension.DATE
--        and sales.SA
)


select * from sales_facts