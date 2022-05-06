CREATE TABLE IF NOT EXISTS  main.terminals (
    time_create DateTime,
    date_balance Date,
    account String,
    name_terminal String,
    type_terminal String,
    caption String,
    sum_national Decimal(18, 2),
    sum_currency Decimal(18, 2),
    date_open Date,
    date_close Nullable(Date)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(date_balance)
ORDER BY (type_terminal, name_terminal)


