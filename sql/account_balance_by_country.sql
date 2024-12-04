WITH balance_per_customer AS (
    SELECT
        c.country,
        t.customer_id,
        COALESCE(SUM(CASE WHEN t.currency = 'GBP' THEN t.amount * er.gbp_to_eur ELSE t.amount END), 0) AS balance
    FROM
        transactions t
        LEFT JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN exchange_rates er ON t.date = er.date
    GROUP BY
        c.country, t.customer_id
)
SELECT
    country,
    SUM(balance) AS total_balance
FROM
    balance_per_customer
GROUP BY
    country
ORDER BY
    total_balance DESC
LIMIT 1;