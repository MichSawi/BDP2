CREATE PROCEDURE GetCurrencyRateYearAgo
    @YearsAgo INT
AS
BEGIN
    DECLARE @TargetDate DATE;
    DECLARE @TargetDateInt INT;

    SET @TargetDate = DATEADD(YEAR, -@YearsAgo, GETDATE());
    SET @TargetDateInt = CAST(FORMAT(@TargetDate, 'yyyyMMdd') AS INT);

    SELECT 
        fcr.DateKey AS Date,         
        fcr.AverageRate,            
        fcr.EndOfDayRate,           
        dc.CurrencyAlternateKey AS CurrencyCode 
    FROM 
        FactCurrencyRate AS fcr
    INNER JOIN 
        DimCurrency AS dc ON fcr.CurrencyKey = dc.CurrencyKey
    WHERE 
        fcr.DateKey < @TargetDateInt  
        AND dc.CurrencyAlternateKey IN ('GBP', 'EUR') 
    ORDER BY 
        fcr.DateKey DESC;
END;
GO