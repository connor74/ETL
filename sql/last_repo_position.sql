SELECT
    SUM(Amount) AS sum_amount,
    round(avgWeighted(RepoRate, Amount), 2) AS avg_rate
FROM main.moex_deals AS deals
JOIN (
SELECT
     TradeNo
FROM main.moex_deals
WHERE
    and(
        RepoPart = 2,
        DueDate > yesterday( ),
        report_date <= yesterday( )
    )
) AS num ON deals.TradeNo = num.TradeNo
WHERE deals.RepoPart = 1