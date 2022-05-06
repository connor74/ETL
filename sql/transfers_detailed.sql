SELECT
    trf.ID AS id,
    FORMAT(trf.DateTransfer, 'yyyy-MM-dd hh:mm:ss')  AS date_transfer,
    ISNULL(ac_debit.IDClient, 0) AS id_client_debit,
    ISNULL(main.IDDebitBank, 0) AS id_bank_debit,
    ISNULL(main.DebitAccount, '00000000000000000000') AS account_debit,
    ac_debit.Caption AS account_debit_title,
    ISNULL(ac_credit.IDClient, 0) AS id_client_credit,
    ISNULL(main.IDCreditBank, 0) AS id_bank_credit,
    ISNULL(main.CreditAccount, '00000000000000000000') AS account_credit,
    ac_credit.Caption AS account_credit_title,
    trf.Summa AS sum_cur,
    trf.SummaNational AS sum_rur,
    main.Purpose AS payment_details,
    ISNULL(main.TypeDoc, 0) AS type_doc,
    ISNULL(main.WhoCreated, '') AS who_created
FROM tla_Transfer as trf (nolock)
    LEFT JOIN tla_Account AS ac_debit (nolock) ON trf.IDAccountDebit = ac_debit.ID
    LEFT JOIN tla_Account AS ac_credit (nolock) ON trf.IDAccountCredit = ac_credit.ID
    LEFT JOIN tpd_Main AS main (nolock) ON main.ID = trf.IDPayDoc
WHERE
    trf.DateTransfer = ?