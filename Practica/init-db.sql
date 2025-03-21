-- Create Database if it does not exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'VuelosDB')
BEGIN
    CREATE DATABASE VuelosDB;
END
GO

-- Use the database
USE VuelosDB;
GO

-- Grant permissions to 'sa'
ALTER DATABASE VuelosDB SET TRUSTWORTHY ON;
EXEC sp_addsrvrolemember 'sa', 'db_owner';
GO
