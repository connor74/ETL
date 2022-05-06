SELECT
	ac.ID AS id_account,
	GETDATE() AS time_create,
	bal.Date AS date_balance,
	ISNULL(FORMAT(ac.TimeChange, 'yyyy-MM-dd hh:mm:ss'), NULL) AS ac_time_change,
	ISNULL(ac.IDClient, 0) AS id_client,
	LEFT(ac.Account, 5) AS account_short,
	LEFT(ac.Account, 3) AS account_short_3,
	SUBSTRING(ac.Account, 4, 2) AS account_short_2,
    CASE
        WHEN SUBSTRING(ac.Account, 6, 3) = '810' THEN 'RUR'
        WHEN SUBSTRING(ac.Account, 6, 3) = '840' THEN 'USD'
        WHEN SUBSTRING(ac.Account, 6, 3) = '978' THEN 'EUR'
		WHEN SUBSTRING(ac.Account, 6, 3) = '156' THEN 'CNY'
		WHEN SUBSTRING(ac.Account, 6, 3) = '398' THEN 'KZT'
		WHEN SUBSTRING(ac.Account, 6, 3) = '826' THEN 'GBP'
        ELSE 'ANY'
    END AS cur_code,
 	ac.Account AS account,
 	bal.Cur AS sum_currency,
	bal.Nat AS sum_national,
	ISNULL(ac.DateOpen, '1970-01-01') AS date_open,
	ISNULL(ac.DateClose, '1970-01-01')  AS date_close,
	ISNULL(FORMAT(tvk.DateEnd, 'yyyy-MM-dd'), '1970-01-01')  AS date_dep_end,
	ISNULL(tvk.DayForAccount, 0) AS days_deposit,
    ISNULL(rate.CurrentPercent, 0) AS rate_dep
FROM tla_Bal_Ex AS bal (nolock)
LEFT JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
LEFT JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
LEFT JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
WHERE bal.Date = ? AND  ac.Account IS NOT NULL