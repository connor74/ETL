select
	GETDATE() AS time_create,
	bal.Date AS date_balance,
	ac.Account AS account,
	TRIM(SUBSTRING(ac.Caption, 0, CHARINDEX('(', ac.Caption))) as name_terminal,
	TRIM(SUBSTRING(ac.Caption, 0, CHARINDEX('_', ac.Caption))) as type_terminal,
	ac.Caption AS caption,
	bal.Nat AS sum_national,
	bal.Cur AS sum_currency,
	ISNULL(ac.DateOpen, '1970-01-01') AS date_open,
	ISNULL(ac.DateClose, '1970-01-01')  AS date_close
FROM tla_Bal_Ex AS bal (nolock)
JOIN tla_Account AS ac (nolock) ON ac.ID = bal.IDAccount
WHERE Account LIKE '20208%' AND bal.Date = ?