/*
====================================================================
 DATA WAREHOUSE INITIALIZATION SCRIPT
====================================================================

Project    : SQL Data Warehouse Project
Environment: Microsoft SQL Server

--------------------------------------------------------------------
PURPOSE
--------------------------------------------------------------------
Initializes the Data Warehouse by:
1. Dropping the existing 'DataWarehouse' database (if it exists)
2. Creating a new database
3. Creating schemas:
   - bronze (raw data)
   - silver (cleaned data)
   - gold   (analytical layer)

--------------------------------------------------------------------
WARNING
--------------------------------------------------------------------
⚠ This action permanently deletes the existing database.
⚠ Ensure backups are available before execution.

--------------------------------------------------------------------
USAGE
--------------------------------------------------------------------
Run in SQL Server Management Studio (SSMS) with required permissions.

====================================================================
*/

USE master;
GO

-- DROP AND RECREATE the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN	
	ALTER DATABASE DataWarehouse 
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

	DROP DATABASE DataWarehouse;
END;
GO

-- Create database
CREATE DATABASE DataWarehouse;
GO  

-- Switch to database
USE DataWarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
