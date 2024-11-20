SELECT OrderDate, COUNT(*) AS OrdersCount
FROM AdventureWorksDW2019.dbo.FactInternetSales
GROUP BY OrderDate
HAVING COUNT(*) < 100
ORDER BY OrdersCount DESC;

WITH DailyTopProducts AS (
    SELECT 
        OrderDate,
        ProductKey,
        UnitPrice,
        ROW_NUMBER() OVER(PARTITION BY OrderDate ORDER BY UnitPrice DESC) AS ProductRank
    FROM AdventureWorksDW2019.dbo.FactInternetSales
)
SELECT 
    OrderDate,
    ProductKey,
    UnitPrice
FROM DailyTopProducts
WHERE ProductRank <= 3;