
{{ config(materialized='table') }}

SELECT
    order_id,
    order_item_id,
    product_key,
    seller_key,
    price,
    freight_value,
    price + freight_value AS total_item_value
FROM dw.fact_order_items
