WITH movment AS (
	SELECT
		IDAccount,
		Date,
		DebitCirculationNational,
		DebitCirculationCurrency,
		CreditCirculationNational,
		CreditCirculationCurrency
	FROM tla_Balance (nolock)
)
SELECT
	CAST(ac.ID AS int) AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	bal.Date AS date_balance,
	CAST(ac.TimeChange AS DATE) AS ac_date_change,
	CAST(ac.IDClient AS bigint) AS client_id,
 	ISNULL(TRIM(cl.BriefName), '') AS client_name,
 	ISNULL(TRIM(cl.INN), '') AS client_inn,
	LEFT(ac.Account, 5) AS account_short,
    CASE
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '810' THEN 'RUR'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '840' THEN 'USD'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '978' THEN 'EUR'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '156' THEN 'CNY'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '398' THEN 'KZT'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = '826' THEN 'GBP'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = 'A98' THEN 'GLD'
    	WHEN SUBSTRING(ac.Account, 6, 3)  = 'A99' THEN 'SLV'
    	ELSE 'NAN'
    END AS cur_code,
 	ac.Account AS account,
 	CAST(
	 	CASE
	 		WHEN ac.[Type] IN ('A', 'А') THEN 1
	 		WHEN ac.[Type] = 'П' THEN 2
	 		WHEN ac.[Type] IN ('AП', 'АП') THEN 3
	 		ELSE 0
	 	END
	AS tinyint) AS account_type,
 	ISNULL(TRIM(ac.Caption), '') AS caption,
 	bal.Nat AS sum_national,
 	bal.Cur AS sum_currency,
 	ISNULL(bal_movment.DebitCirculationNational, 0.0) AS debit_national,
 	ISNULL(bal_movment.DebitCirculationCurrency, 0.0) AS debit_currency,
  	ISNULL(bal_movment.CreditCirculationNational, 0.0) AS credit_national,
 	ISNULL(bal_movment.CreditCirculationCurrency, 0.0) AS credit_currency,
	ISNULL(CAST(ac.DateOpen AS smalldatetime), '1979-01-01') AS date_open,
	ISNULL(CAST(ac.DateClose AS smalldatetime), '1970-01-01')  AS date_close,
	ISNULL(tvk.DateEnd, '1970-01-01') AS date_dep_end,
	ISNULL(CAST(tvk.DayForAccount AS smallint), 0) AS days_deposit,
    ISNULL(rate.CurrentPercent, 0.0) AS rate_dep
FROM tla_Bal_Ex AS bal (nolock)
LEFT JOIN movment AS bal_movment (nolock) ON bal.IDAccount = bal_movment.IDAccount AND bal.[Date] = bal_movment.Date
LEFT JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
LEFT JOIN tvk_Main AS tvk (nolock) ON tvk.Account = ac.Account
LEFT JOIN trk_Rate AS rate (nolock) ON rate.ID = tvk.IDRate
LEFT JOIN tcln_Main AS cl (nolock) ON cl.ID = ac.IDClient
WHERE bal.Date = ? AND  ac.Account IS NOT NULL
ORDER BY id_account;