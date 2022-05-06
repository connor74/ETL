CREATE OR REPLACE VIEW main.view_bonds_table AS
SELECT
    gr_data.SecurityId,
    main.bonds_history.short_name,
    main.bonds_history.mat_date,
    main.bonds_history.offer_date,
    gr_data.quant,
    main.bonds_history.face_value,
    main.bonds_history.market_price_3,
    main.bonds_history.accint,
    main.bonds_history.coupon_value
FROM (
    SELECT
        SecurityId,
        SUM(IF( BuySell IN ('S', 'E'), toInt64( Quantity ) * (-1), toInt64( Quantity ) )) AS quant
    FROM main.moex_deals
    WHERE and(
        PriceType = 'PERC',
        RepoPart = 0,
        ClientCode IS NULL,
        ExchComm = 0,
        BoardId != 'BCS'
    )
    GROUP BY SecurityId
    HAVING quant > 0
) AS gr_data
LEFT JOIN main.bonds_history ON main.bonds_history.sec_id = gr_data.SecurityId
WHERE main.bonds_history.trade_date = (
    SELECT MAX(trade_date) FROM main.bonds_history
)
ORDER BY main.bonds_history.short_name