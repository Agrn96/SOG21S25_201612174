USE VuelosDB;
GO

-- Drop existing tables if they exist
DROP TABLE IF EXISTS Flights_Fact;
GO
DROP TABLE IF EXISTS Passengers_Dim;
GO
DROP TABLE IF EXISTS Airports_Dim;
GO
DROP TABLE IF EXISTS Pilots_Dim;
GO
DROP TABLE IF EXISTS Dates_Dim;
GO
DROP TABLE IF EXISTS FlightStatus_Dim;
GO

-- Dimension Table: Passengers
CREATE TABLE Passengers_Dim (
    PassengerID INT PRIMARY KEY IDENTITY(1,1),
    OriginalPassengerID VARCHAR(50) COLLATE Latin1_General_BIN2 UNIQUE,
    FirstName VARCHAR(100),
    LastName VARCHAR(100),
    Gender VARCHAR(10),
    Age INT,
    Nationality VARCHAR(50)
);
GO

-- Dimension Table: Airports
CREATE TABLE Airports_Dim (
    AirportID INT PRIMARY KEY IDENTITY(1,1),
    AirportCode VARCHAR(3) UNIQUE NOT NULL,
    AirportName VARCHAR(100) NOT NULL,
    CountryCode VARCHAR(10),
    CountryName VARCHAR(50),
    Continent VARCHAR(50)
);
GO

-- Dimension Table: Dates (Departure Dates)
CREATE TABLE Dates_Dim (
    DateKey INT PRIMARY KEY IDENTITY(1,1),
    FullDate DATE UNIQUE NOT NULL,
    Year INT,
    Month INT,
    Day INT
);
GO

-- Dimension Table: Flight Status
CREATE TABLE FlightStatus_Dim (
    FlightStatusID INT PRIMARY KEY IDENTITY(1,1),
    FlightStatus VARCHAR(20) UNIQUE NOT NULL
);
GO

-- Dimension Table: Pilots
CREATE TABLE Pilots_Dim (
    PilotID INT PRIMARY KEY IDENTITY(1,1),
    PilotName VARCHAR(100) UNIQUE NOT NULL
);
GO

-- Fact Table: Flights
CREATE TABLE Flights_Fact (
    FlightID INT PRIMARY KEY IDENTITY(1,1),
    PassengerID INT FOREIGN KEY REFERENCES Passengers_Dim(PassengerID),
    DateKey INT FOREIGN KEY REFERENCES Dates_Dim(DateKey),
    AirportID INT FOREIGN KEY REFERENCES Airports_Dim(AirportID),
    PilotID INT FOREIGN KEY REFERENCES Pilots_Dim(PilotID),
    FlightStatusID INT FOREIGN KEY REFERENCES FlightStatus_Dim(FlightStatusID)
);
GO
