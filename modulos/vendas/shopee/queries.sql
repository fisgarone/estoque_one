-- Consulta para KPIs principais
SELECT
  SUM(valor_total) AS total_revenue,
  SUM(lucro_real) AS total_profit,
  AVG(lucro_real_pct) * 100 AS avg_margin,
  (COUNT(CASE WHEN status_pedido = 'Cancelado' THEN 1 END) * 100.0 / COUNT(*) AS cancel_rate
FROM vendas_shopee
WHERE data BETWEEN ? AND ?;

-- Consulta para faturamento diário
SELECT
  DATE(data) AS day,
  SUM(valor_total) AS daily_revenue
FROM vendas_shopee
WHERE data BETWEEN ? AND ?
GROUP BY day
ORDER BY day;

-- Consulta para top produtos
SELECT
  nome_item AS product_name,
  SUM(lucro_real) AS total_profit
FROM vendas_shopee
WHERE data BETWEEN ? AND ?
GROUP BY product_name
ORDER BY total_profit DESC
LIMIT 5;

-- Consulta para transportadoras
SELECT
  transportadora AS shipping_company,
  COUNT(*) AS orders_count
FROM vendas_shopee
WHERE data BETWEEN ? AND ?
GROUP BY shipping_company
ORDER BY orders_count DESC;

-- Consulta para tendência mensal
SELECT
  strftime('%Y-%m', data) AS month,
  SUM(valor_total) AS monthly_revenue,
  SUM(lucro_real) AS monthly_profit
FROM vendas_shopee
WHERE data BETWEEN ? AND ?
GROUP BY month
ORDER BY month
LIMIT 12;

-- Consulta para últimas vendas
SELECT
  pedido_id,
  data,
  nome_item,
  qtd_comprada,
  valor_total,
  lucro_real,
  status_pedido
FROM vendas_shopee
WHERE data BETWEEN ? AND ?
ORDER BY data DESC
LIMIT 10;