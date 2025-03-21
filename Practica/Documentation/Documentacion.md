# **Documentación del Modelo de Datos - VuelosDB**

## **Introducción**
El modelo de datos **VuelosDB** ha sido diseñado para gestionar y analizar información de vuelos, pasajeros, aeropuertos, pilotos y estados de vuelo en un entorno de inteligencia de negocios. Este modelo sigue un enfoque de **modelo dimensional (Data Warehouse)** utilizando **Esquema Estrella** para facilitar el análisis de datos mediante consultas eficientes y optimizadas.

---

## **Descripción de las Tablas**

### **2.1 Tablas Dimensionales** (Dimensiones)
Las tablas dimensionales contienen información descriptiva que se usa para el análisis de datos en la tabla de hechos.

### **`Passengers_Dim` (Dimensión de Pasajeros)**
| Campo            | Tipo de Dato     | Descripción |
|-----------------|----------------|-------------|
| `PassengerID`   | INT (PK)        | Identificador único del pasajero. |
| `OriginalPassengerID` | VARCHAR(50) | Identificador original del pasajero en los datos fuente. |
| `FirstName`     | VARCHAR(100)    | Nombre del pasajero. |
| `LastName`      | VARCHAR(100)    | Apellido del pasajero. |
| `Gender`        | VARCHAR(10)     | Género del pasajero (Male/Female). |
| `Age`           | INT             | Edad del pasajero. |
| `Nationality`   | VARCHAR(50)     | Nacionalidad del pasajero. |

### **`Airports_Dim` (Dimensión de Aeropuertos)**
| Campo         | Tipo de Dato | Descripción |
|--------------|-------------|-------------|
| `AirportID`  | INT (PK)    | Identificador único del aeropuerto. |
| `AirportCode`| VARCHAR(3)  | Código IATA del aeropuerto. |
| `AirportName`| VARCHAR(100)| Nombre del aeropuerto. |
| `CountryCode`| VARCHAR(10) | Código del país donde se encuentra el aeropuerto. |
| `CountryName`| VARCHAR(50) | Nombre del país donde se encuentra el aeropuerto. |
| `Continent`  | VARCHAR(50) | Continente en el que se encuentra el aeropuerto. |

### **`Pilots_Dim` (Dimensión de Pilotos)**
| Campo     | Tipo de Dato | Descripción |
|----------|-------------|-------------|
| `PilotID` | INT (PK)    | Identificador único del piloto. |
| `PilotName` | VARCHAR(100) | Nombre del piloto. |

### **`Dates_Dim` (Dimensión de Fechas)**
| Campo     | Tipo de Dato | Descripción |
|----------|-------------|-------------|
| `DateKey`  | INT (PK)    | Clave única de la fecha en formato `YYYYMMDD`. |
| `FullDate` | DATE        | Fecha completa en formato `YYYY-MM-DD`. |
| `Year`     | INT         | Año de la fecha. |
| `Month`    | INT         | Mes de la fecha. |
| `Day`      | INT         | Día de la fecha. |
| `MonthName`| VARCHAR(20) | Nombre del mes. |
| `Quarter`  | INT         | Trimestre del año. |

### **`FlightStatus_Dim` (Dimensión de Estado de Vuelo)**
| Campo         | Tipo de Dato | Descripción |
|--------------|-------------|-------------|
| `FlightStatusID` | INT (PK) | Identificador único del estado del vuelo. |
| `FlightStatus` | VARCHAR(20) | Estado del vuelo (On Time, Delayed, Cancelled). |

---

### **2.2 Tabla de Fact**
Esta tabla almacena las relaciones entre las dimensiones y los datos de vuelos.

### **`Flights_Fact`**
| Campo          | Tipo de Dato | Descripción |
|--------------|-------------|-------------|
| `FlightID`   | INT (PK)    | Identificador único del vuelo. |
| `PassengerID` | INT (FK)    | Relación con `Passengers_Dim`. |
| `AirportID`   | INT (FK)    | Relación con `Airports_Dim`. |
| `PilotID`     | INT (FK)    | Relación con `Pilots_Dim`. |
| `DateKey`     | INT (FK)    | Relación con `Dates_Dim`. |
| `FlightStatusID` | INT (FK) | Relación con `FlightStatus_Dim`. |

---

## **Relaciones entre Tablas**

- **`Flights_Fact`** es la tabla principal que relaciona todas las dimensiones.
- **Cada vuelo está asociado con un pasajero, un aeropuerto, un piloto, una fecha y un estado de vuelo.**
- **Las claves foráneas (`FK`) garantizan la integridad referencial entre la tabla de hechos y las dimensiones.**

---

## **Justificación del Diseño del Modelo**
- **Facilita análisis de datos en BI**: Permite ejecutar consultas analíticas rápidas y eficientes.
- **Optimización en almacenamiento**: Se evita la redundancia de datos mediante dimensiones normalizadas.
- **Flexibilidad**: Se pueden agregar nuevas dimensiones sin afectar la estructura central.
- **Escalabilidad**: El modelo permite el crecimiento sin comprometer el rendimiento.

---
