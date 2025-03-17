with time_source as(
    select * from {{ source('tpch', 'time') }}
),

time_dimension as(
    select 

        {{ dbt_utils.surrogate_key(
            ['time_source.FISCAL_YEAR'],['time_source.YEAR'],['time_source.MONTH'],['time_source.DAY']) }}
                as time_key,
        time_source.FISCAL_YEAR,
        time_source.YEAR,
        time_source.MONTH,
        time_source.DAY,
        time_source.DATE,
        time_source.WEEKDAY,
        time_source.HOLIDAY_FLG

    from time_source
)

select * from time_dimension