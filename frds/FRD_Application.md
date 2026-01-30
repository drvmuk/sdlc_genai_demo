# Functional Requirements Document — Application

Here is the detailed Functional Requirement Document (FRD) based on the provided BRD's "Coding Requirements" section.

### 1. Requirement ID
* FRD-001: Load Customer and Order Data into Delta Tables
* FRD-002: Clean and Process Customer and Order Data
* FRD-003: Create ordersummary Table and Load Joined Data
* FRD-004: Implement SCD Type 2 Logic for ordersummary Table
* FRD-005: Create customeraggregatespend Table and Load Aggregated Data

### 2. Title
* Loading and Processing Customer and Order Data
* Generating Aggregated Customer Spend Data

### 3. Description
The application will read customer and order data from CSV files, clean and process the data, and then load it into Delta tables. It will then join the customer and order data and load it into an SCD type 2 table. Finally, it will aggregate the data and load it into a customeraggregatespend table.

### 4. Preconditions
* The CSV files for customer and order data are available at the specified locations.
* The Delta tables for customer and order data do not exist or are empty.
* The catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard" exist.

### 5. Main Flow / Functional Steps

* **Step 1: Load Customer and Order Data into Delta Tables**
  • Read customer data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
  • Read order data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
  • Load customer data into Delta table `customer` with schema: `CustId`, `Name`, `EmailId`, `Region`
  • Load order data into Delta table `order` with schema: `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`, `CustId`

* **Step 2: Clean and Process Customer and Order Data**
  • Remove null and duplicate records from `customer` and `order` tables

* **Step 3: Create ordersummary Table and Load Joined Data**
  • Create `ordersummary` table if not exists in catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard" with schema: `CustId`, `Name`, `EmailId`, `Region`, `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`
  • Join `customer` and `order` data using `CustId` field
  • Load joined data into `ordersummary` table as SCD type 2 table

* **Step 4: Implement SCD Type 2 Logic for ordersummary Table**
  • Update `ordersummary` table whenever there is a change in `customer` table
  • Make old records inactive and new records active
  • Update `StartDate` and `EndDate` accordingly

* **Step 5: Create customeraggregatespend Table and Load Aggregated Data**
  • Create `customeraggregatespend` table if not exists in catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard" with columns: `Name`, `TotalAmount`, `Date`
  • Aggregate `TotalAmount` from `ordersummary` table and group by `Name` and `Date`
  • Load aggregated data into `customeraggregatespend` table