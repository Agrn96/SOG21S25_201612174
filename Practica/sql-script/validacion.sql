# Comprobar si todas las tablas existen
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN ('Passengers_Dim', 'Airports_Dim', 'Pilots_Dim', 'Dates_Dim', 'FlightStatus_Dim', 'Flights_Fact');

# Verificar claves primarias en cada tabla
SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME IN ('Passengers_Dim', 'Airports_Dim', 'Pilots_Dim', 'Dates_Dim', 'FlightStatus_Dim', 'Flights_Fact')
AND CONSTRAINT_NAME LIKE 'PK%';

# Validar claves foráneas
SELECT 
    fk.name AS CONSTRAINT_NAME,
    OBJECT_NAME(fkc.parent_object_id) AS TABLE_NAME,
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS COLUMN_NAME,
    OBJECT_NAME(fkc.referenced_object_id) AS REFERENCED_TABLE_NAME
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id;

# Contar registros en cada tabla
SELECT 'Passengers_Dim' AS TableName, COUNT(*) AS RecordCount FROM Passengers_Dim
UNION ALL
SELECT 'Airports_Dim', COUNT(*) FROM Airports_Dim
UNION ALL
SELECT 'Pilots_Dim', COUNT(*) FROM Pilots_Dim
UNION ALL
SELECT 'Dates_Dim', COUNT(*) FROM Dates_Dim
UNION ALL
SELECT 'FlightStatus_Dim', COUNT(*) FROM FlightStatus_Dim
UNION ALL
SELECT 'Flights_Fact', COUNT(*) FROM Flights_Fact;

# Detectar registros huérfanos en `Flights_Fact`
SELECT f.FlightID
FROM Flights_Fact f
LEFT JOIN Passengers_Dim p ON f.PassengerID = p.PassengerID
LEFT JOIN Airports_Dim a ON f.AirportID = a.AirportID
LEFT JOIN Pilots_Dim pl ON f.PilotID = pl.PilotID
LEFT JOIN Dates_Dim d ON f.DateKey = d.DateKey
LEFT JOIN FlightStatus_Dim fs ON f.FlightStatusID = fs.FlightStatusID
WHERE p.PassengerID IS NULL OR a.AirportID IS NULL OR pl.PilotID IS NULL OR d.DateKey IS NULL OR fs.FlightStatusID IS NULL;

# Validar porcentaje de pasajeros por género
SELECT Gender, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Passengers_Dim) AS Percentage
FROM Passengers_Dim
GROUP BY Gender;

# Validar número de vuelos por país
SELECT a.CountryName, COUNT(*) AS TotalFlights
FROM Airports_Dim a
JOIN Flights_Fact f ON a.AirportID = f.AirportID
GROUP BY a.CountryName
ORDER BY TotalFlights DESC;


# Medir el tiempo de ejecución de consultas críticas
SET STATISTICS TIME ON;

SELECT TOP 5 a.AirportName, COUNT(*) AS TotalPassengers
FROM Flights_Fact f
JOIN Airports_Dim a ON f.AirportID = a.AirportID
GROUP BY a.AirportName
ORDER BY TotalPassengers DESC;

SET STATISTICS TIME OFF;