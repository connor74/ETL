CREATE OR REPLACE VIEW main.view_median_business_acc AS
SELECT
    date_balance,
    median( sum_national ) AS median,
    avg( sum_national ) AS avg
FROM main.balance
WHERE or(account_short = '407%', account_short = '406%', account_short = '40802')
GROUP BY date_balance