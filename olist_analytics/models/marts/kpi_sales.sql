
{{ config(materialized='table') }}

SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_order_items,
    SUM(price) AS total_product_sales,
    SUM(freight_value) AS total_freight,
    SUM(total_item_value) AS total_sales_with_freight,
    AVG(price) AS average_item_price
FROM {{ ref('fct_order_items_custom') }}
