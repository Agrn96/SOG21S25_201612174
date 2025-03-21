IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'VuelosDB')
BEGIN
    CREATE DATABASE VuelosDB;
END
GO
USE VuelosDB;
GO
