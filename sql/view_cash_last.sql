CREATE OR REPLACE VIEW main.view_cash_last AS
WITH (SELECT MAX(date_balance) FROM main.balance) AS last_date_balance
SELECT
    substring(account, 6, 3) AS code,
    SUM(sum_currency) * (-1) AS sum_currency,
    SUM(sum_national) * (-1) AS sum_national,
    'ГО' AS office
FROM main.balance
WHERE
    and(
        account IN (
            '20202810500000001000',
            '20202826400000001000',
            '20202840800000001000',
            '20202978400000001000',
            '20202398200000001000',
            '20202156400000001000',
            '20202810800000002000',
            '20202826700000002000',
            '20202840100000002000',
            '20202978700000002000',
            '20202156700000002000'
        ),
        date_balance = last_date_balance
    )
GROUP BY code
UNION ALL
SELECT
    substring(account, 6, 3) AS code,
    SUM(sum_currency) * (-1) AS sum_currency,
    SUM(sum_national) * (-1) AS sum_national,
    'ДО 1' AS office
FROM main.balance
WHERE
    and(
        account IN (
            '20202826420100000004',
            '20202840800010000004',
            '20202978400010000004',
            '20202810500010000004'
        ),
        date_balance = last_date_balance
    )
GROUP BY code
UNION ALL
SELECT
    substring(account, 6, 3) AS code,
    SUM(sum_currency) * (-1) AS sum_currency,
    SUM(sum_national) * (-1) AS sum_national,
    'ДО 4' AS office
FROM main.balance
WHERE
    and(
        account IN (
            '20202840500040000015',
            '20202978100040000015',
            '20202810200040000015'
        ),
        date_balance = last_date_balance
    )
GROUP BY code
UNION ALL
SELECT
    substring(account, 6, 3) AS code,
    SUM(sum_currency) * (-1) AS sum_currency,
    SUM(sum_national) * (-1) AS sum_national,
    'ДО 5' AS office
FROM main.balance
WHERE
    and(
        account IN (
            '20202840020500000019',
            '20202978620500000019',
            '20202810720500000019'
        ),
        date_balance = last_date_balance
    )
GROUP BY code