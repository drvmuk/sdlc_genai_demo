# Functional Requirements Document — Application

### Functional Requirement Document (FRD)

#### 1. Requirement ID
- FRD-001: Load Customer and Order Data
- FRD-002: Clean Customer and Order Data
- FRD-003: Create ordersummary Table
- FRD-004: Load Data into ordersummary Table
- FRD-005: Implement SCD Type 2 Logic for ordersummary Table
- FRD-006: Create customeraggregatespend Table
- FRD-007: Aggregate and Load Data into customeraggregatespend Table

#### 2. Title
Data Processing and Aggregation for Customer and Order Data

#### 3. Description
This document outlines the functional requirements for processing customer and order data, including data cleaning, loading into a delta table, implementing Slowly Changing Dimension (SCD) Type 2 logic, and aggregating spend data by customer.

#### 4. Preconditions
- The source CSV files for customer and order data are available at the specified paths.
- The necessary catalog and schema exist in the Databricks environment.

#### 5. Main Flow / Functional Steps

* **Step 1: Read Source CSV Data and Load to Delta Tables**
  • Read customer data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata` and load into a delta table named `customer`.
  • Read order data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata` and load into a delta table named `order`.

* **Step 2: Clean Customer and Order Data**
  • Remove records with null values from both `customer` and `order` tables.
  • Remove duplicate records from both `customer` and `order` tables.

* **Step 3: Create ordersummary Table if Not Exists**
  • Create a table named `ordersummary` under `catalog="gen_ai_poc_databrickscoe"` and `schema="sdlc_wizard"` if it does not exist.
  • The schema of `ordersummary` should be: `CustId`, `Name`, `EmailId`, `Region`, `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`.

* **Step 4: Load Data into ordersummary Table**
  • Join `customer` and `order` data on the `CustId` field.
  • Load the joined data into the `ordersummary` table, implementing SCD Type 2 logic.

* **Step 5: Implement SCD Type 2 Logic for ordersummary Table**
  • When there is a change in the `customer` table, update the `ordersummary` table accordingly.
  • For updated records, mark the old records as inactive and new records as active.
  • Update `StartDate` and `EndDate` for the records in `ordersummary` accordingly.

* **Step 6: Create customeraggregatespend Table if Not Exists**
  • Create a table named `customeraggregatespend` under `catalog="gen_ai_poc_databrickscoe"` and `schema="sdlc_wizard"` if it does not exist.
  • The schema of `customeraggregatespend` should be: `Name`, `TotalAmount`, `Date`.

* **Step 7: Aggregate and Load Data into customeraggregatespend Table**
  • Aggregate the `TotalAmount` (computed as `PricePerUnit` * `Qty`) from the `ordersummary` table, grouped by `Name` and `Date`.
  • Load the aggregated data into the `customeraggregatespend` table.

#### OUTPUT
Upon receiving the input PDF or DOCX file name, return a short summary of the custom template and ask if the user wants to use the default or custom FRD template.

After generating the FRD:
- If the user chooses to push the FRD to GitHub, proceed with the action.
- Ask: "Would you like me to push this FRD to GitHub as a Markdown file from memory?"