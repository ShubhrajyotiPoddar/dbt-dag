select
    orders.order_key,
    line_items.line_number,
    orders.order_date,
    orders.customer_key,
    line_items.order_item_key,
    line_items.quantity,
    line_items.extended_price,
    line_items.discount_percentage,
    line_items.tax_rate,
    {{discounted_amount('line_items.extended_price', 'line_items.discount_percentage')}} as item_discount_amount
from 
    {{ ref('stg_tpch_orders') }} as orders
    join {{ ref('stg_tpch_line_items') }} as line_items
    on orders.order_key = line_items.order_key
order by
    orders.order_date