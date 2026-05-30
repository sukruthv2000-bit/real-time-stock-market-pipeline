-- 1. Daily stock performance
SELECT
    event_date,
    ticker,
    open_price,
    close_price,
    daily_return_pct,
    total_volume
FROM daily_stock_summary
ORDER BY event_date, ticker;

-- 2. Top gainers
SELECT
    event_date,
    ticker,
    daily_return_pct
FROM daily_stock_summary
ORDER BY daily_return_pct DESC;

-- 3. Highest volume stocks
SELECT
    event_date,
    ticker,
    total_volume
FROM daily_stock_summary
ORDER BY total_volume DESC;

-- 4. Volatility proxy
SELECT
    event_date,
    ticker,
    high_price - low_price AS intraday_range
FROM daily_stock_summary
ORDER BY intraday_range DESC;

-- 5. Average closing price
SELECT
    ticker,
    AVG(close_price) AS avg_close_price
FROM daily_stock_summary
GROUP BY ticker
ORDER BY avg_close_price DESC;
