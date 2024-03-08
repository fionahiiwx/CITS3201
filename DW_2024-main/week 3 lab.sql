SELECT * FROM FactSalesQuota;
SELECT * FROM FactCallCenter;

DROP TABLE IF EXISTS NewFactSalesQuota;
CREATE TABLE NewFactSalesQuota AS
SELECT *
FROM FactSalesQuota
ORDER BY datekey, salesamountquota;
SELECT * FROM NewFactSalesQuota;

DROP TABLE IF EXISTS NewFactCallCenter;
CREATE TABLE NewFactCallCenter AS
SELECT *
FROM FactCallCenter
ORDER BY shift, averagetimeperissue;
SELECT * FROM NewFactCallCenter;

SELECT *
FROM DimCustomer;

DROP TABLE IF EXISTS NewDimCustomer;
CREATE TABLE NewDimCustomer AS
SELECT *
FROM DimCustomer
ORDER BY 
	gender, birthdate, 
	maritalstatus, yearlyincome, 
	totalchildren, numberchildrenathome, 
	houseownerflag, datefirstpurchase, 
	commutedistance;
SELECT * FROM NewDimCustomer;

SELECT *
FROM DimProduct;

DROP TABLE IF EXISTS NewDimProduct;
CREATE TABLE NewDimProduct AS
SELECT *
FROM DimProduct
ORDER BY 
	status, safetystocklevel, sizerange, standardcost;
SELECT * FROM NewDimProduct;