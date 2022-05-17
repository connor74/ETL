SELECT
	CAST(ac.ID AS int) AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	CAST(ac.TimeChange AS DATE) AS ac_date_change,
	CAST(ac.IDClient AS bigint) AS client_id,
 	ISNULL(TRIM(cl.BriefName), '') AS client_name,
 	ISNULL(TRIM(cl.INN), '') AS client_inn,
	LEFT(ac.Account, 5) AS account_short,
    SUBSTRING(ac.Account, 6, 3) AS cur_code,
 	ac.Account AS account,
 	ISNULL(TRIM(ac.Caption), '') AS caption,
	ISNULL(CAST(ac.DateOpen AS smalldatetime), '1979-01-01') AS date_open,
	ISNULL(CAST(ac.DateClose AS smalldatetime), '1970-01-01')  AS date_close,
	ISNULL(tvk.DateEnd, '1970-01-01') AS date_dep_end,
	ISNULL(CAST(tvk.DayForAccount AS smallint), 0) AS days_deposit,
    ISNULL(rate.CurrentPercent, 0.0) AS rate_dep
FROM tla_Account AS ac (nolock)
LEFT JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
LEFT JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
LEFT JOIN tcln_Main AS cl (nolock) ON cl.ID = ac.IDClient
WHERE ac.Account IS NOT NULL AND CAST(ac.TimeChange AS Date) = ?
ORDER BY ac.Account;