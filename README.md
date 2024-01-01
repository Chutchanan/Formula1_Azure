# Formula1 Data Engineering Project

**1. Project Overview**

```
This is a dedicated learning project with a primary focus on utilizing Azure Databricks for in-depth data
processing and analysis of Formula One racing data. The project encompassed various data engineering
and visualization tools, providing an immersive learning experience in data engineering.
```
```
Data source for this project: https://ergast.com/mrd/
```
```
Key Technologies
```
- Azure Databricks: Leveraged Azure Databricks for advanced data extraction, transformation, and
    analysis, enabling a deep dive into data engineering techniques.
- Azure Data Factory (ADF): Utilized Azure Data Factory for orchestrating data workflows, including data
    ingestion and transformation processes.
- PowerBI: Implemented PowerBI for interactive data visualization, facilitating hands-on exploration of
    data engineering and visualization tools.

```
Project Architecture
```
- Azure Data Lake Storage (ADLS) is used for storing data in various stages from raw to presentation.
- Databricks is involved in the ingestion, transformation, and analysis phase.
- ADF is used to automate the data pipeline workflows, coordinating the movement of data across various
    stages, and invoking processing tasks, such as Azure Databricks notebooks, for data ingestion,
    transformation and analysis.
- PowerBI is a visualization tool that connects to the processed data to create reports and dashboards.


**Entity Relationship Diagram**

1. **drivers** : Contains information about the drivers such as driver ID, driver reference, number, code, forename,
    surname, date of birth (dob), nationality, and URL for more details.
2. **constructors** : Holds data about the teams (constructors), including an ID, reference, name, nationality, and
    URL.
3. **circuits** : Lists the racing circuits with details such as circuit ID, reference, name, location, country, latitude
    (lat), longitude (lng), altitude (alt), and URL.
4. **seasons** : Stores information about each racing season, including the year and a URL for more details.
5. **races** : Central to the database, this table contains records of each race including the race ID, year, round,
    circuit ID, name, date, time, and various URLs for detailed data and results.
6. **qualifying** : Details the qualifying results for races, with information on the position and times achieved in
    different qualifying sessions (Q1, Q2, Q3).
7. **sprintResults** : If applicable, stores the results of sprint qualifying races, with information about positions,
    points, times, and status.
8. **results** : Contains the final race results, including the race ID, driver ID, constructor ID, position, points, status,
    and timing details like fastest laps.
9. **driverStandings** : Tracks the driver standings at various points in the season, including points, position, and
    wins.
10. **constructorStandings** : Similar to driverStandings, but for the constructors (teams).
11. **constructorResults** : Records the results of constructors in races.
12. **pitStops** : Logs details about each pit stop, such as the race, driver, stop number, lap, time, duration, and the
    total time in milliseconds.
13. **lapTimes** : Records the lap times for each driver during a race, with details like the race ID, driver ID, lap
    number, position, and the time taken.
14. **status** : A reference table that explains the status codes used in other tables, like results or sprintResults, to
    describe the outcome of a race for a driver (e.g., finished, collision, engine failure).


**2. Project Requirements**

```
Data Ingestion
```
- Ingest All 8 files into the data lake.
- Ingested data must have audit columns.
- Ingested data must be stored in columnar format (i.e., Parquet).
- Ingested data must have the schema applied.
- Must be able to analyze the ingested data via SQL.
- Ingestion logic must be able to handle incremental load.

```
Data Transformation
```
- Join the key information required for reporting to create a new table.
- Transformed data must be stored in columnar format (i.e., Parquet).
- Must be able to analyze the transformed data via SQL.
- Transformation logic must be able to handle incremental load.

```
Report
```
- Driver standings
- Constructor standings

```
Scheduling
```
- Scheduled to run every Sunday at 10PM.
- Ability to monitor pipelines.
- Ability to re-run failed pipelines.
- Ability to set up alerts for failures.


**3. Project Implementation**

3.1 Mounting databricks to ADLS so that we can access the ADLS directly by databricks.
1. Create a Service Principal: Create a service principal in Azure Active Directory, which will be used for
    authentication without interactive login.
2. Assign Permissions: Assign the necessary permissions to the service principal for ADLS access, in this
    case 'Storage Blob Data Contributor'.
3. Store Credentials in Azure Key Vault: Place the service principal's credentials (tenant ID, client ID, and
    client secret) in Azure Key Vault for secure storage and management.
4. Configure Databricks: Within Databricks, set up the configuration to authenticate with ADLS using the
    stored credentials. Link Azure Key Vault with Databricks (Secret scope) to securely retrieve the secrets.
5. Mount ADLS: Use a Databricks notebook to mount the ADLS filesystem using Databricks File System
    (DBFS) commands. Utilize the credentials retrieved from Azure Key Vault for this process.

3.2 Data Ingestion
All data are uploaded to ADLS in various file formats to learn different types of ingestion.

3.2.1 Ingest all eight files into our process container.
- Circuits – csv
- Races – csv
- Constructors – single line json
- Results – single line json
- Drivers – nested json
- Pitstops – multiline json
- Lap Times – split csv files
- Qualifying – split multiline json files

3.2.2 Apply correct schema with appropriate column names and data types.

3.2.3 Include audit columns to track the ingestion date.

3.2.4 Store ingested data as Parquet files for various workloads, including machine learning, reporting, and SQL analytics.

3.2.5 Ensure the logic can handle incremental data.

3.3 Data Transformation
Joined all necessary tables (results, races, circuits, drivers, constructors) to create race result table and store in presentation layer.


```
3.4 Data Analysis
```
Query data from race result from a presentation layer to calculate dominant drivers and
constructors and make a visualization.

```
Dominant drivers
```
```
Dominant teams
```

```
3.5 Data Pipeline
```
I have constructed a comprehensive 'Process Pipeline' within Azure Data Factory (ADF), which
effectively encapsulates and manages the functions of data ingestion and data transformation pipelines.
This pipeline incorporates a 'Get Metadata' activity to retrieve and verify folder names against 'If Condition'
parameters. This comparison is critical to ensure the 'window end date' derived from the tumbling window
trigger corresponds with the intended folder name. Upon a successful match, the 'Process Pipeline'
proceeds to execute a series of Databricks notebooks. If there is no match, the execution of the pipeline is
bypassed, optimizing my processing efficiency.

```
Ingestion Pipeline
```
```
Transformation Pipeline
```
```
Process Pipeline
```

