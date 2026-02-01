# Functional Requirements Document — Application

Here is the detailed Functional Requirement Document (FRD) based on the "Coding Requirements" section of the BRD:


### 1. Requirement ID
• FRD-001: Load Customer and Order Data
• FRD-002: Remove Null and Duplicate Records
• FRD-003: Create ordersummary Table
• FRD-004: Load ordersummary Table
• FRD-005: Implement SCD Type 2 Logic
• FRD-006: Create customeraggregatespend Table
• FRD-007: Load customeraggregatespend Table

### 2. Title
• Load Customer and Order Data into Delta Tables
• Clean and Process Customer and Order Data
• Create and Load ordersummary Table
• Implement SCD Type 2 Logic for ordersummary Table
• Create and Load customeraggregatespend Table

### 3. Description
The application will read customer and order data from CSV files, clean and process the data, and load it into Delta tables. It will then create and load the ordersummary table using SCD Type 2 logic and create and load the customeraggregatespend table with aggregated data.

### 4. Preconditions
• Customer and order data CSV files are available at the specified locations.
• The Delta tables customer and order are created.

### 5. Main Flow / Functional Steps

  • Read customer data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
  • Read order data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
  • Load customer data into Delta table `customer` with schema: `CustId, Name, EmailId, Region`
  • Load order data into Delta table `order` with schema: `OrderId, ItemName, PricePerUnit, Qty, Date, CustId`

  • Remove null and duplicate records from `customer` table
  • Remove null and duplicate records from `order` table

  • Create `ordersummary` table if not exists in catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard` with schema: `CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date`

  • Join `customer` and `order` tables using `CustId` field
  • Load joined data into `ordersummary` table using SCD Type 2 logic

  • Update `ordersummary` table whenever there is a change in `customer` table
  • Make old records inactive and new records active
  • Update `StartDate` and `EndDate` accordingly

  • Create `customeraggregatespend` table if not exists in catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard` with columns: `Name, TotalAmount, Date`

  • Aggregate `TotalAmount` column from `ordersummary` table and group by `Name` and `Date` columns
  • Load aggregated data into `customeraggregatespend` table
