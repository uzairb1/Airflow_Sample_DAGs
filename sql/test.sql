WITH transactions_with_conversion AS (
    SELECT
        t.customer_id,
        c.country,
        STRFTIME('%m', t.date) AS month,
        t.date,
        COALESCE(SUM(CASE WHEN t.currency = 'GBP' THEN t.amount * er.gbp_to_eur ELSE t.amount END), 0) AS amount_eur
    FROM
        transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN exchange_rates er ON t.date = er.date
    WHERE
        date(t.date) >= '2019-01-01'
    GROUP BY
        c.country,  t.date, month
)
, monthly_transactions AS (
    SELECT
        country,
        strftime('%m', date) AS month,
        date,
        SUM(amount_eur) AS monthly_total
    FROM
        transactions_with_conversion
    WHERE
        date >= '2019-01-01 00:00:00'
    GROUP BY
        country, month, date
)
, cumulative_monthly AS (
    SELECT
        country,
        month,
        date,
        SUM(monthly_total) OVER (PARTITION BY country ORDER BY month) AS cumulative_balance
    FROM
        monthly_transactions
)
SELECT
    country,
    date(date),
    cumulative_balance AS total_balance
FROM
    cumulative_monthly
ORDER BY
    country, date;