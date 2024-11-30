{{ config(
   materialized="table"
) }}

SELECT
  item_purchased,
  SUM(Purchase_Amount) Total_Purchase,
  COUNT(DISTINCT Customer_ID) Total_Customer_Purchased
FROM {{ ref("dw_shopping_data") }}
GROUP BY 1