/* 
===============================================================
Create Database and Schemas
===============================================================

Script Purpose:
   This script creates a new databse named as 'DataWarehouse' after checking if it's already exists. 
   If it database exists, then dropped and recreate. Additionally, the script sets up three scehmas 
   within the database : 'bronze', 'silver', and 'gold'.

WARNING:
      Running this script will drop entire 'DataWarehouse' database if exists.
      All data in database will permanently deleted. Proceed with caution and 
      ensure you have proper backups before running this script.

*/



-- Create Database 'DataWarehouse'
Use master;
GO

-- Drop & recreate the 'DataWarehouse' database if exists
IF Exists (Select 1 from sys.databases where name = 'DataWarehouse')
Begin 
     Alter Database DataWarehouse Set single_user with rollback immediate;
	 Drop Database DataWarehouse;
End;
GO


-- Create 'DataWarehouse' database 
Create Database DataWarehouse;
Go

Use DataWarehouse;

-- Create Schemas

Create Schema bronze;
GO
Create Schema silver;
Go
Create Schema gold;
Go
