{{ config(
   materialized="table"
) }}

SELECT 
  `Customer ID` As Customer_ID, 
  Location,
  Age,
  Gender,
  `Purchase Amount` as Purchase_Amount,
  `Previous Purchases` as Previous_Purchases,
  `Frequency of Purchases` as Frequency_of_Purchases,
  `Item Purchased` as Item_Purchased,
  Size,
  Color,
  `Review Rating` as Review_Rating,
  `Subscription Status` as Subscription_Status,
  `Shipping Type` as Shipping_Type,
  `Payment Method` as Payment_Method,
  `Preferred Payment Method` as Preferred_Payment_Method,
  Season,
  `Promo Code Used` as Promo_Code_Used,
  `Discount Applied` as Discount_Applied
FROM {{ source('staging','sg_shopping_data') }} 
