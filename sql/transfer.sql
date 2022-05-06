SELECT
    trf.ID AS id,
    FORMAT(trf.DateTransfer, 'yyyy-MM-dd hh:mm:ss')  AS date_transfer,
    ISNULL(ac_debit.IDClient, 0) AS id_client_debit,
    ISNULL(main.IDDebitBank, 0) AS id_bank_debit,
    LEFT(ISNULL(main.DebitAccount, '00000000000000000000'), 20) AS account_debit_20,
    ISNULL(main.DebitAccount, '00000000000000000000') AS account_debit,
    ISNULL(ac_credit.IDClient, 0) AS id_client_credit,
    ISNULL(main.IDCreditBank, 0) AS id_bank_credit,
    LEFT(ISNULL(main.CreditAccount, '00000000000000000000'), 20) AS account_credit_20,
    ISNULL(main.CreditAccount, '00000000000000000000') AS account_credit,
    trf.Summa AS summa,
    trf.SummaNational AS sum_national,
    ISNULL(main.TypeDoc, 0) AS type_doc,
    ISNULL(main.WhoCreated, '') AS who_created
FROM tla_Transfer as trf (nolock)
    LEFT JOIN tla_Account AS ac_debit (nolock) ON trf.IDAccountDebit = ac_debit.ID
    LEFT JOIN tla_Account AS ac_credit (nolock) ON trf.IDAccountCredit = ac_credit.ID
    LEFT JOIN tpd_Main AS main (nolock) ON main.ID = trf.IDPayDoc
WHERE
    trf.DateTransfer = ?
