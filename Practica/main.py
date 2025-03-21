import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "sql-script", "data")

os.makedirs(DATA_DIR, exist_ok=True)

def connect_db(DATA_BASE):
    """ Establish a connection to SQL Server """
    try:
        conn = pyodbc.connect(
            f"DRIVER={os.getenv('DB_DRIVER')};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={DATA_BASE};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASSWORD')}"
        )
        return conn
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        return None

def execute_sql_script(file_path, DATA_BASE):
    """ Execute SQL script from a file """
    conn = connect_db(DATA_BASE)
    conn.autocommit = True
    cursor = conn.cursor()

    with open(file_path, "r") as file:
        sql_script = file.read()

    statements = sql_script.split("GO")  

    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
            except pyodbc.Error as e:
                print(f"[ERROR] Failed executing statement: {statement.strip()}\n{e}")

    conn.commit()
    conn.close()
    print(f"[INFO] Executed SQL script: {file_path}")
    
QUERIES = {
    "1": ("Total Records in Each Table", 
          "SELECT 'Passengers_Dim' AS TableName, COUNT(*) AS RecordCount FROM Passengers_Dim "
          "UNION ALL SELECT 'Flights_Fact', COUNT(*) FROM Flights_Fact "
          "UNION ALL SELECT 'Airports_Dim', COUNT(*) FROM Airports_Dim "
          "UNION ALL SELECT 'Pilots_Dim', COUNT(*) FROM Pilots_Dim "
          "UNION ALL SELECT 'Dates_Dim', COUNT(*) FROM Dates_Dim "
          "UNION ALL SELECT 'FlightStatus_Dim', COUNT(*) FROM FlightStatus_Dim;"),

    "2": ("Percentage of Passengers by Gender", 
          "SELECT Gender, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Passengers_Dim) AS Percentage "
          "FROM Passengers_Dim GROUP BY Gender;"),

    "3": ("Nationalities with Highest Departures by Month", 
          "WITH MonthlyCounts AS ("
          "    SELECT p.Nationality, FORMAT(d.FullDate, 'MM-yyyy') AS MonthYear, COUNT(*) AS TotalFlights "
          "    FROM Passengers_Dim p "
          "    JOIN Flights_Fact f ON p.PassengerID = f.PassengerID "
          "    JOIN Dates_Dim d ON f.DateKey = d.DateKey "
          "    GROUP BY p.Nationality, FORMAT(d.FullDate, 'MM-yyyy') "
          ") "
          "SELECT * FROM MonthlyCounts "
          "PIVOT ("
          "    SUM(TotalFlights) FOR MonthYear IN ([01-2022], [02-2022], [03-2022], [04-2022], [05-2022], "
          "                                          [06-2022], [07-2022], [08-2022], [09-2022], [10-2022], "
          "                                          [11-2022], [12-2022])"
          ") AS PivotTable "
          "ORDER BY Nationality;"),

    "4": ("Number of Flights per Country", 
          "SELECT a.CountryName, COUNT(*) AS TotalFlights "
          "FROM Airports_Dim a JOIN Flights_Fact f ON a.AirportID = f.AirportID "
          "GROUP BY a.CountryName ORDER BY TotalFlights DESC;"),

    "5": ("Top 5 Airports with Most Passengers", 
          "SELECT TOP 5 a.AirportName, COUNT(*) AS TotalPassengers "
          "FROM Flights_Fact f JOIN Airports_Dim a ON f.AirportID = a.AirportID "
          "GROUP BY a.AirportName ORDER BY TotalPassengers DESC;"),

    "6": ("Count of Flights by Flight Status", 
          "SELECT fs.FlightStatus, COUNT(*) AS TotalFlights "
          "FROM Flights_Fact f JOIN FlightStatus_Dim fs ON f.FlightStatusID = fs.FlightStatusID "
          "GROUP BY fs.FlightStatus;"),

    "7": ("Top 5 Most Visited Countries", 
          "SELECT TOP 5 a.CountryName, COUNT(*) AS TotalVisits "
          "FROM Airports_Dim a JOIN Flights_Fact f ON a.AirportID = f.AirportID "
          "GROUP BY a.CountryName ORDER BY TotalVisits DESC;"),

    "8": ("Top 5 Most Visited Continents", 
          "SELECT TOP 5 a.Continent, COUNT(*) AS TotalVisits "
          "FROM Airports_Dim a JOIN Flights_Fact f ON a.AirportID = f.AirportID "
          "GROUP BY a.Continent ORDER BY TotalVisits DESC;"),

    "9": ("Top 5 Most Traveling Age Groups by Gender", 
          "SELECT TOP 5 p.Age, p.Gender, COUNT(*) AS TotalPassengers "
          "FROM Passengers_Dim p "
          "GROUP BY p.Age, p.Gender "
          "ORDER BY TotalPassengers DESC;"),

    "10": ("Count of Flights per MM-YYYY", 
           "SELECT d.Year, d.Month, COUNT(*) AS TotalFlights "
           "FROM Flights_Fact f JOIN Dates_Dim d ON f.DateKey = d.DateKey "
           "GROUP BY d.Year, d.Month ORDER BY d.Year, d.Month;"),
}

def run_query(choice):
    conn = connect_db("VuelosDB")
    cursor = conn.cursor()
    
    query_title, query_sql = QUERIES[choice]
    
    print(f"\n[INFO] Running Query: {query_title}\n")
    cursor.execute(query_sql)
    
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    conn.close()

def run_queries():
    while True:
        print("\n===== Analytical Queries Menu =====")
        for key, value in QUERIES.items():
            print(f"{key}. {value[0]}")
        print("11. Go Back to Main Menu")
        
        choice = input("Select a query to run: ")

        if choice in QUERIES:
            run_query(choice)
        elif choice == "11":
            break
        else:
            print("[ERROR] Invalid option. Try again.")

def disable_foreign_keys(cursor):
    """ Disable foreign key checks before bulk insert """
    cursor.execute("ALTER TABLE dbo.Flights_Fact NOCHECK CONSTRAINT ALL;")
    print("[INFO] Foreign key constraints disabled.")

def enable_foreign_keys(cursor):
    """ Re-enable foreign key checks after bulk insert """
    cursor.execute("ALTER TABLE dbo.Flights_Fact CHECK CONSTRAINT ALL;")
    print("[INFO] Foreign key constraints re-enabled.")
    
def transform_and_save_csvs():
    file_path = input("Enter the CSV file path: ")

    if not os.path.exists(file_path):
        print("[ERROR] File not found.")
        return

    try:
        df = pd.read_csv(file_path)

        def parse_dates(date_str):
            for fmt in ('%m/%d/%Y', '%m-%d-%Y'):
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except ValueError:
                    continue
            return pd.NaT 

        df["Departure Date"] = df["Departure Date"].apply(parse_dates)

        if df["Departure Date"].isna().sum() > 0:
            print("[WARNING] Some dates could not be parsed. Check the dataset.")

        df["Departure Date"] = df["Departure Date"].dt.strftime("%Y-%m-%d")


        # Departure Date Dimension
        dim_departure_date = df[['Departure Date']].drop_duplicates().copy()
        dim_departure_date['DepartureDateID'] = range(1, len(dim_departure_date) + 1)
        dim_departure_date['Year']  = pd.to_datetime(dim_departure_date['Departure Date']).dt.year
        dim_departure_date['Month'] = pd.to_datetime(dim_departure_date['Departure Date']).dt.month
        dim_departure_date['Day']   = pd.to_datetime(dim_departure_date['Departure Date']).dt.day
        print("[INFO] Departure Date Dimension created.")

        # Airport Dimension 
        dim_airport = df[['Arrival Airport','Airport Name', 'Airport Country Code', 'Country Name', 'Airport Continent']].drop_duplicates().copy()
        dim_airport['AirportID'] = range(1, len(dim_airport) + 1)
        dim_airport = dim_airport.drop_duplicates(subset=['Arrival Airport'])
        print("[INFO] Airport Dimension created.")

        # Pilot Dimension
        dim_pilot = df[['Pilot Name']].drop_duplicates().copy()
        dim_pilot['PilotID'] = range(1, len(dim_pilot) + 1)
        print("[INFO] Pilot Dimension created.")

        # Flight Status Dimension
        dim_flight_status = df[['Flight Status']].drop_duplicates().copy()
        dim_flight_status['FlightStatusID'] = range(1, len(dim_flight_status) + 1)
        print("[INFO] Flight Status Dimension created.")


        # Passenger Dimension
        dim_passenger = df[['Passenger ID', 'First Name', 'Last Name', 'Gender', 'Age', 'Nationality']].drop_duplicates().copy()
        dim_passenger = dim_passenger.drop_duplicates(subset=['Passenger ID'])
        dim_passenger['PassengerID'] = range(1, len(dim_passenger) + 1)
        print("[INFO] Passenger Dimension created.")
        
        # --- Create Fact Table ---
        df['PassengerID'] = df['Passenger ID'].map(dim_passenger.set_index('Passenger ID')['PassengerID'])
        df['DepartureDateID'] = df['Departure Date'].map(dim_departure_date.set_index('Departure Date')['DepartureDateID'])
        df['AirportID'] = df['Arrival Airport'].map(dim_airport.set_index('Arrival Airport')['AirportID'])
        df['PilotID'] = df['Pilot Name'].map(dim_pilot.set_index('Pilot Name')['PilotID'])
        df['FlightStatusID'] = df['Flight Status'].map(dim_flight_status.set_index('Flight Status')['FlightStatusID'])
        print("[INFO] Fact Table created.")

        # Fact Table
        fact_flight = df[['PassengerID', 'DepartureDateID', 'AirportID', 'PilotID', 'FlightStatusID']].copy()
        
        print("[INFO] Data transformed successfully.")
        conn = connect_db("VuelosDB")
        print("[INFO] Connected to SQL Server.")
        if conn:
            cursor = conn.cursor()
            disable_foreign_keys(cursor)

            print("[INFO] Inserting data into SQL Server...")

            try:
                cursor.execute("USE VuelosDB;")
                conn.commit()
                print("[INFO] Using VuelosDB database")
                cursor.fast_executemany = True
                cursor.executemany("INSERT INTO dbo.Passengers_Dim (OriginalPassengerID, FirstName, LastName, Gender, Age, Nationality) VALUES (?, ?, ?, ?, ?, ?)", 
                                dim_passenger[['Passenger ID', 'First Name', 'Last Name', 'Gender', 'Age', 'Nationality']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into Passengers_Dim")

                cursor.executemany("INSERT INTO dbo.Airports_Dim (AirportCode, AirportName, CountryCode, CountryName, Continent) VALUES (?, ?, ?, ?, ?)", 
                                dim_airport[['Arrival Airport', 'Airport Name', 'Airport Country Code', 'Country Name', 'Airport Continent']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into Airports_Dim")

                cursor.executemany("INSERT INTO dbo.Dates_Dim (FullDate, Year, Month, Day) VALUES (?, ?, ?, ?)", 
                                dim_departure_date[['Departure Date', 'Year', 'Month', 'Day']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into Dates_Dim")

                cursor.executemany("INSERT INTO dbo.Pilots_Dim (PilotName) VALUES (?)", 
                                dim_pilot[['Pilot Name']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into Pilots_Dim")

                cursor.executemany("INSERT INTO dbo.FlightStatus_Dim (FlightStatus) VALUES (?)", 
                                dim_flight_status[['Flight Status']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into FlightStatus_Dim")

                cursor.executemany("INSERT INTO dbo.Flights_Fact (PassengerID, DateKey, AirportID, PilotID, FlightStatusID) VALUES (?, ?, ?, ?, ?)", 
                                fact_flight[['PassengerID', 'DepartureDateID', 'AirportID', 'PilotID', 'FlightStatusID']].values.tolist())
                conn.commit()
                print("[INFO] Data inserted into Flights_Fact")
            
                # Re-enable foreign key checks after bulk insert
                enable_foreign_keys(cursor)

            except Exception as e:
                print(f"[ERROR] Failed inserting data: {e}")
                conn.rollback()

            conn.close()
            print("[INFO] Data successfully inserted into SQL Server.")

        dim_passenger.to_csv(os.path.join(DATA_DIR, "dim_passenger.csv"), index=False)
        dim_departure_date.to_csv(os.path.join(DATA_DIR, "dim_departure_date.csv"), index=False)
        dim_airport.to_csv(os.path.join(DATA_DIR, "dim_airport.csv"), index=False)
        dim_pilot.to_csv(os.path.join(DATA_DIR, "dim_pilot.csv"), index=False)
        dim_flight_status.to_csv(os.path.join(DATA_DIR, "dim_flight_status.csv"), index=False)
        fact_flight.to_csv(os.path.join(DATA_DIR, "fact_flight.csv"), index=False)

        print(f"[INFO] Transformed data saved to {DATA_DIR}")
        
        print("DimPassenger:")
        print(dim_passenger.head())
        print("Total registros:", len(dim_passenger))
        input("Presione Enter para continuar...")

        print("DimDepartureDate:")
        print(dim_departure_date.head())
        print("Total registros:", len(dim_departure_date))
        input("Presione Enter para continuar...")

        print("DimAirport:")
        print(dim_airport.head())
        print("Total registros:", len(dim_airport))
        input("Presione Enter para continuar...")

        print("DimPilot:")
        print(dim_pilot.head())
        print("Total registros:", len(dim_pilot))
        input("Presione Enter para continuar...")

        print("DimFlightStatus:")
        print(dim_flight_status.head())
        print("Total registros:", len(dim_flight_status))
        input("Presione Enter para continuar...")

        print("FactFlight:")
        print(fact_flight.head())
        print("Total registros:", len(fact_flight))
        input("Presione Enter para continuar...")

    except Exception as e:
        print(f"[ERROR] Failed to process CSV: {e}")

    # execute_sql_script("sql-script/load_data.sql", "VuelosDB")

def main():
    execute_sql_script("sql-script/create_db.sql", "master")
    
    while True:
        print("\n===== ETL Console Application =====")
        print("1. Delete Existing Model") 
        print("2. Create BI Model in SQL Server")
        print("3. Extract & Preview Data from CSV")
        print("4. Run Analytical Queries")
        print("5. Exit")

        choice = input("Select an option: ")

        if choice == "1":
            execute_sql_script("sql-script/delete_schema.sql", "VuelosDB")
        elif choice == "2":
            execute_sql_script("sql-script/create_schema.sql", "VuelosDB")
        elif choice == "3":
            transform_and_save_csvs()
        elif choice == "4":
            run_queries()
        elif choice == "5":
            print("Exiting application...")
            break
        else:
            print("[ERROR] Invalid option. Try again.")

if __name__ == "__main__":
    main()
