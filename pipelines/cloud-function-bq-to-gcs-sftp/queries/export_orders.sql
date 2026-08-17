SELECT
  o.order_id,
  o.order_date,
  c.customer_id,
  c.customer_name,
  p.product_id,
  p.product_name,
  o.quantity,
  o.unit_price,
  o.quantity * o.unit_price AS gross_amount,
  CASE
    WHEN c.customer_tier = 'GOLD' THEN 'PRIORITY'
    WHEN o.quantity * o.unit_price >= 1000 THEN 'HIGH_VALUE'
    ELSE 'STANDARD'
  END AS order_classification,
  CASE
    WHEN o.status = 'SHIPPED' THEN 'COMPLETED'
    WHEN o.status IN ('CANCELLED', 'RETURNED') THEN 'EXCEPTION'
    ELSE 'OPEN'
  END AS order_status_group
FROM `{orders_table}` AS o
INNER JOIN `{customers_table}` AS c
  ON o.customer_id = c.customer_id
LEFT JOIN `{products_table}` AS p
  ON o.product_id = p.product_id
WHERE o.order_date BETWEEN @start_date AND @end_date
  AND o.is_test_order = FALSE
  AND o.status NOT IN ('DELETED', 'DRAFT')
  AND c.is_active = TRUE
