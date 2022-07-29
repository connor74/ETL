SELECT
	CAST(ac.ID AS int) AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	CAST(ac.TimeChange AS smalldatetime) AS ac_time_change,
	ac.Account  AS account,
	CAST(ac.IDClient AS bigint) AS client_id,
	ac.IDCurrency AS id_currency,
	ac.Caption AS ac_caption,
 	ISNULL(TRIM(cl.BriefName), '') AS client_name,
 	ISNULL(TRIM(cl.INN), '') AS client_inn,
	CAST(ac.DateOpen AS smalldatetime) AS date_open,
	CAST(ac.DateClose AS smalldatetime)  AS date_close,
	CAST(ac.DateNeedOpen AS smalldatetime)  AS date_need_open,
	CAST(ac.DateNeedClose AS smalldatetime)  AS date_need_close
FROM tla_Account AS ac (nolock)
LEFT JOIN tcln_Main AS cl (nolock) ON cl.ID = ac.IDClient
WHERE ac.Account IS NOT NULL AND ac.TimeChange > ?
ORDER BY ac_time_change;