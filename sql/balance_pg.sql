SELECT
    [Date],
    IDAccount,
    Nat*100 AS sum_national,
    Cur*100 AS sum_currency
FROM tla_Bal_Ex WHERE [Date] = ?;