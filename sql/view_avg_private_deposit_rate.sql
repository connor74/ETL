CREATE OR REPLACE VIEW main.view_avg_private_deposit_rate AS
SELECT
date_balance,
round(avgWeighted(rate, sum_national), 4) AS avg_rate
FROM
main.pr_deposits
WHERE
    and(
        date_balance > '2018-01-01',
        account NOT LIKE '42301%',
        SUBSTRING(account, 6, 3) = '810'
    )
GROUP BY date_balance