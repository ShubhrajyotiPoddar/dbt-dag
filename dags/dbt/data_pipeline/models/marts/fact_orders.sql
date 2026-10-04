select
    orders.*,
    order_items_summary.total_extended_price,
    order_items_summary.item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('int_order_items_summary') }} as order_items_summary
on
    orders.order_key = order_items_summary.order_key
order by
    orders.order_date