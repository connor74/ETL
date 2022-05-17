SELECT
	bal.IDAccount  AS id_account,
	CAST(GETDATE() AS smalldatetime) AS time_create,
	bal.Date AS date_balance,
 	bal.Cur AS sum_currency,
	bal.Nat AS sum_national
FROM tla_Bal_Ex AS bal (nolock)
WHERE bal.Date = ?
ORDER BY date_balance, id_account;