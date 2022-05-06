SELECT TOP 100
	ac.ID AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	bal.Date AS date_balance,
	CAST(ac.TimeChange AS smalldatetime) AS ac_time_change,
	CAST(ac.IDClient AS bigint) AS client_id,
 	TRIM(cl.BriefName) AS client_name,
 	TRIM(cl.INN) AS client_inn,
	LEFT(ac.Account, 5) AS account_short,
	LEFT(ac.Account, 3) AS account_short_3,
	SUBSTRING(ac.Account, 4, 2) AS account_short_2,
    SUBSTRING(ac.Account, 6, 3) AS cur_code,
 	ac.Account AS account,
 	TRIM(ac.Caption) AS caption,
 	ac.[Type] ,
 	bal.Cur AS sum_currency,
	bal.Nat AS sum_national,
	CAST(ac.DateOpen AS smalldatetime) AS date_open,
	CAST(ac.DateClose AS smalldatetime)  AS date_close,
	CAST(tvk.DateEnd AS smalldatetime) AS date_dep_end,
	tvk.DayForAccount AS days_deposit,
    rate.CurrentPercent AS rate_dep
FROM tla_Bal_Ex AS bal (nolock)
LEFT JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
LEFT JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
LEFT JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
LEFT JOIN tcln_Main AS cl (nolock) ON cl.ID = ac.IDClient
WHERE bal.Date = ? AND  ac.Account IS NOT NULL