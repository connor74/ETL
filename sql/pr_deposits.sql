SELECT
    ac.ID AS id,
    GETDATE() AS time_create,
    ac.IDClient AS id_client,
    FORMAT(bal.Date, 'yyyy-MM-dd') AS date_balance,
    FORMAT(ac.TimeChange, 'yyyy-MM-dd hh:mm:ss') AS ac_time_change,
    ac.Account AS account,
    ISNULL(ac.DateOpen, '1970-01-01') AS date_open,
    ISNULL(FORMAT(ac.DateClose, 'yyyy-MM-dd'), '1970-01-01') AS date_close,
    ISNULL(FORMAT(tvk.DateEnd, 'yyyy-MM-dd'), '1970-01-01')  AS date_end,
    SUBSTRING(ac.Account, 12, 1) AS office,
    CASE
        WHEN SUBSTRING(ac.Account, 6, 3) = '810' THEN 'RUR'
        WHEN SUBSTRING(ac.Account, 6, 3) = '840' THEN 'USD'
        WHEN SUBSTRING(ac.Account, 6, 3) = '978' THEN 'EUR'
        ELSE 'ANY'
    END AS cur_code,
    bal.Nat as sum_national,
    bal.Cur as sum_currency,
    person.Pol AS sex,
    ISNULL(tvk.DayForAccount, 0) AS days_deposit,
    rate.CurrentPercent AS rate
FROM tla_Bal_Ex AS bal (nolock)
    JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
    JOIN tm_Man AS person (nolock) ON person.IDClient = ac.IDClient
    JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
    JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
WHERE
    bal.Date = ? AND
    ac.Account LIKE '423[0-7]%'
ORDER BY bal.Date