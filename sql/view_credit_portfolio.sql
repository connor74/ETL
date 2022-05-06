CREATE OR REPLACE VIEW main.view_credit_portfolio AS
WITH (SELECT MAX(date_balance) FROM main.balance) AS last_date
SELECT
    date_balance,
    multiIf(
        match(account_short, '^45(0|1|2|3|4)0(0|1|2|3|4|5|6|7|8|9)'), 'ЮЛ',
        or(
            account_short LIKE '4550_',
            account_short IN ('45510','45511')
        ), 'ФЛ',
        'other'
    ) AS category,
    SUM(sum_national) AS summa
FROM
    main.balance
WHERE
    date_balance > '2018-01-01' AND
    account_short LIKE '45___'


GROUP BY
    date_balance,
    category

http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/RU000A1003E6.json?from=2021-12-30&till=2022-01-12