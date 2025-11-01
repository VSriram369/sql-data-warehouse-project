-- Create DataWareHouse

-- Step 1 
USE master;

-- Step 2
CREATE DATABASE Datawarehouse;

-- Step 3
USE Datawarehouse;

-- Step 4
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
