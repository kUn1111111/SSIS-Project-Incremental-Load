# SSIS Project 1 — Incremental Load (Delta Load) for FactInternetSales

Purpose
- Provide a single, authoritative README that contains the SQL to create the helper tables and stored procedures, and step‑by‑step SSIS instructions so the repository contents "fit the README".
- Implement an incremental (delta) load of new rows from dbo.FactInternetSales into a staging table using OrderDateKey as the watermark.
- Maintain a simple tracking table with LastOrderDateKey and a load log table with Started / Succeeded / Failed statuses.

Quick overview
- Staging table: dbo.InternetSales_Staging
- Tracking table: dbo.InternetSales_LoadTracking (stores LastOrderDateKey)
- Logging table: dbo.InternetSales_LoadLog (records Started, Succeeded, Failed)
- SSIS Control Flow: Start Log → (optional: Get LastOrderDateKey) → Data Flow (Source → Staging) → Update LoadTracking → Success Log
  Use an OnError Event Handler (or red precedence constraints) to insert a Failure Log.

Prerequisites
- SQL Server with dbo.FactInternetSales table accessible.
- SSIS development environment (Visual Studio / SSDT) with OLE DB connections.
- Permissions to create tables and stored procedures in the target database.

SQL: create tables and optional stored procedures
- Run the following in your target DB to create the staging, tracking and logging tables and helpful stored procedures.

```sql
-- Create staging, tracking and log tables (idempotent-ish)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'InternetSales_Staging' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.InternetSales_Staging
    (
        SalesOrderNumber NVARCHAR(20),
        CustomerKey INT,
        ProductKey INT,
        OrderDateKey INT,
        SalesAmount DECIMAL(18,2),
        LoadDate DATETIME DEFAULT GETDATE()
    );
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'InternetSales_LoadTracking' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.InternetSales_LoadTracking
    (
        LastOrderDateKey INT
    );
    INSERT INTO dbo.InternetSales_LoadTracking (LastOrderDateKey) VALUES (0);
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'InternetSales_LoadLog' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.InternetSales_LoadLog
    (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        LoadDate DATETIME DEFAULT GETDATE(),
        Status NVARCHAR(50),  -- 'Started', 'Succeeded', 'Failed'
        Message NVARCHAR(4000) NULL
    );
END
```

Optional stored procedures (recommended for clearer Execute SQL Tasks)
```sql
-- Insert load log
IF OBJECT_ID('dbo.usp_InsertLoadLog', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_InsertLoadLog;
GO
CREATE PROCEDURE dbo.usp_InsertLoadLog
    @Status NVARCHAR(50),
    @Message NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.InternetSales_LoadLog (Status, Message) VALUES (@Status, @Message);
END
GO

-- Get last OrderDateKey
IF OBJECT_ID('dbo.usp_GetLastOrderDateKey', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_GetLastOrderDateKey;
GO
CREATE PROCEDURE dbo.usp_GetLastOrderDateKey
    @LastOrderDateKey INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP 1 @LastOrderDateKey = LastOrderDateKey FROM dbo.InternetSales_LoadTracking;
END
GO

-- Update tracking from staging
IF OBJECT_ID('dbo.usp_UpdateLastOrderDateKeyFromStaging', 'P') IS NOT NULL
    DROP PROCEDURE dbo.usp_UpdateLastOrderDateKeyFromStaging;
GO
CREATE PROCEDURE dbo.usp_UpdateLastOrderDateKeyFromStaging
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @maxOrderDateKey INT;
    SELECT @maxOrderDateKey = MAX(OrderDateKey) FROM dbo.InternetSales_Staging;
    IF @maxOrderDateKey IS NOT NULL
    BEGIN
        UPDATE dbo.InternetSales_LoadTracking
        SET LastOrderDateKey = @maxOrderDateKey;
    END
END
GO
```

SSIS package: Control Flow (detailed)
1) Start Log (Execute SQL Task)
- Purpose: Record the run start.
- SQL (example):
  INSERT INTO dbo.InternetSales_LoadLog (Status, Message) VALUES ('Started', 'SSIS package started');

2) (Optional) Get LastOrderDateKey (Execute SQL Task)
- Purpose: Read the watermark into variable User::LastOrderDateKey to avoid subqueries in the source.
- SQL:
  SELECT LastOrderDateKey FROM dbo.InternetSales_LoadTracking
- ResultSet: Single row -> map to User::LastOrderDateKey

3) Data Flow: Load new rows into dbo.InternetSales_Staging
- OLE DB Source
  - Two options:
    a) Use variable (recommended)
       SQL command:
         SELECT SalesOrderNumber, CustomerKey, ProductKey, OrderDateKey, SalesAmount
         FROM dbo.FactInternetSales
         WHERE OrderDateKey > ?
       Parameter 0 -> User::LastOrderDateKey
    b) Use inline subquery:
         SELECT SalesOrderNumber, CustomerKey, ProductKey, OrderDateKey, SalesAmount
         FROM dbo.FactInternetSales
         WHERE OrderDateKey > (SELECT LastOrderDateKey FROM dbo.InternetSales_LoadTracking)
- Transformations
  - Data Conversion / Derived Column as needed to match staging types
  - (Optional) Row Count into User::RowsLoaded
- OLE DB Destination
  - Destination Table: dbo.InternetSales_Staging
  - Use Fast Load and tune batch size/commit size

4) Update LoadTracking (Execute SQL Task)
- After the Data Flow, update the watermark to the highest OrderDateKey loaded:
  UPDATE dbo.InternetSales_LoadTracking
  SET LastOrderDateKey = (SELECT MAX(OrderDateKey) FROM dbo.InternetSales_Staging);
- Or call stored procedure: EXEC dbo.usp_UpdateLastOrderDateKeyFromStaging

5) Success Log (Execute SQL Task)
- Insert a success record:
  INSERT INTO dbo.InternetSales_LoadLog (Status, Message) VALUES ('Succeeded', 'SSIS package finished successfully');

Error handling
- Add an OnError Event Handler at package level (or use red precedence constraints) to insert a failure log:
  INSERT INTO dbo.InternetSales_LoadLog (Status, Message) VALUES ('Failed', ?)
  - Use System::ErrorDescription or construct a detailed message into a variable and pass it to the Execute SQL Task.
- In Data Flow, configure error outputs for problematic rows and optionally persist rejected rows to a reject table or file.

Variables recommended
- User::LastOrderDateKey (Int32) — watermark read at package start
- User::MaxOrderDateKey (Int32) — optional, max key discovered after load
- User::RowsLoaded (Int32) — optional, from Row Count transform
- User::LoadMessage (String) — for failure details, optional

Best practices & performance tips
- Ensure an index on FactInternetSales(OrderDateKey).
- Use parameterized source (Get LastOrderDateKey into variable) for plan stability and performance.
- Use Fast Load on OLE DB Destination and tune commit size.
- Consider purging or archiving staging based on LoadDate to control table growth.
- If FactInternetSales can change (updates/deletes), implement a MERGE strategy or add columns to detect changes (rowversion/hash).

Testing checklist
- Run create table scripts and verify initial LastOrderDateKey = 0.
- Seed FactInternetSales with test rows of increasing OrderDateKey and run the package.
- Verify:
  - 'Started' log entry inserted at run start
  - Only rows with OrderDateKey > LastOrderDateKey are loaded into staging
  - LastOrderDateKey is updated to the MAX(OrderDateKey) loaded
  - 'Succeeded' log entry inserted on completion
  - On intentional failure, a 'Failed' log entry is inserted with a message

Repository files
- sql/create_tables.sql — contains the table creation SQL (same as README)
- sql/procedures.sql — stored procedures shown above (same as README)
- ssis/ssis_package_instructions.md — step‑by‑step guidance and SSIS property recommendations

If you want a .dtsx skeleton
- I can generate a textual .dtsx skeleton with the Execute SQL Tasks, variables and basic event handler included. Note: the .dtsx XML is long; tell me if you want the full XML file committed.

Contact / next steps
- If you'd like, I will:
  - Commit this README.md into the repo root (I can prepare the commit for you),
  - Or generate and commit the .dtsx skeleton for the SSIS package,
  - Or produce step‑by‑step screenshots and property values for each SSIS component.
