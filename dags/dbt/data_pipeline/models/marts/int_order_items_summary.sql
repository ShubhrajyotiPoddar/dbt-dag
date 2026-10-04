select
    order_key,
    sum(extended_price) as total_extended_price,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key