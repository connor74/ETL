SELECT TOP 5
	CAST(ac.ID AS int) AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	bal.Date AS date_balance,
	CAST(ac.TimeChange AS smalldatetime) AS ac_time_change,
	CAST(ac.IDClient AS bigint) AS client_id,
 	ISNULL(TRIM(cl.BriefName), '') AS client_name,
 	ISNULL(TRIM(cl.INN), '') AS client_inn,
	LEFT(ac.Account, 5) AS account_short,
	LEFT(ac.Account, 3) AS account_short_3,
	SUBSTRING(ac.Account, 4, 2) AS account_short_2,
    SUBSTRING(ac.Account, 6, 3) AS cur_code,
 	ac.Account AS account,
 	ISNULL(TRIM(ac.Caption), '') AS caption,
 	bal.Cur AS sum_currency,
	bal.Nat AS sum_national,
	ISNULL(CAST(ac.DateOpen AS smalldatetime), '1979-01-01') AS date_open,
	ISNULL(CAST(ac.DateClose AS smalldatetime), '1970-01-01')  AS date_close,
	ISNULL(tvk.DateEnd, '1970-01-01') AS date_dep_end,
	ISNULL(CAST(tvk.DayForAccount AS smallint), 0) AS days_deposit,
    ISNULL(rate.CurrentPercent, 0.0) AS rate_dep
FROM tla_Bal_Ex AS bal (nolock)
LEFT JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
LEFT JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
LEFT JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
LEFT JOIN tcln_Main AS cl (nolock) ON cl.ID = ac.IDClient
WHERE bal.Date = ? AND  ac.Account IS NOT NULL
ORDER BY id_account;