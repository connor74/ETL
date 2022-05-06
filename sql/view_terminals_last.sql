CREATE OR REPLACE VIEW main.view_terminals_last AS
SELECT
    if(type_terminal='ПТ', 'терминал', 'банкомат') AS type,
    caption,
    sum_national * (-1) AS sum_national
FROM main.terminals
WHERE date_balance = (
    SELECT MAX(date_balance) FROM main.terminals
)
ORDER BY sum_national DESC
