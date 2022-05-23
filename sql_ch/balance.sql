CREATE TABLE IF NOT EXISTS main.balance
(
    id_account UInt64,
    time_create DateTime,
    date_balance Date,
    ac_date_change Date,
  	client_id UInt64,
    client_name String,
    client_inn String,
    account_short FixedString(5),
    cur_code FixedString(3),
    account String,
    account_type UInt8,
    caption String,
 	sum_national Decimal(18, 2),
 	sum_currency Decimal(18, 2),
 	debit_national Decimal(18, 2),
 	debit_currency Decimal(18, 2),
  	credit_national Decimal(18, 2),
 	credit_currency  Decimal(18, 2),
   	date_open Date,
    date_close Date,
    date_dep_end Date,
    days_deposit UInt16,
    rate_dep Decimal(16, 2)
)ENGINE = MergeTree
PARTITION BY toYYYYMMDD(date_balance)
ORDER BY (date_balance, account, cur_code);
