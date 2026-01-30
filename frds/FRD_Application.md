# Functional Requirements Document — Application

### OUTPUT
* In the first response when user provided PDF or DOCX file name, you must return the short summary(text) of the custom template just to show this is the custom template like 'Your FRD will contain these sections: 1. Requirement ID, 2. Title, 3. Description, 4. Preconditions, 5. Main Flow / Functional Steps' in the chat only (no JSON) and also ask question "Do you want me to use the default FRD template or a custom template you provide?" after user gives PDF file.
* Then:
* Use clear headings, short paragraphs, and bullet lists.

### Functional Requirements Document (FRD)

#### 1. Requirement ID
* FRD-001: Load customer and order data from CSV files to Delta tables.
* FRD-002: Remove null and duplicate records from customer and order tables.
* FRD-003: Create ordersummary table if not exists.
* FRD-004: Join customer and order data and load into SCD type 2 ordersummary table.
* FRD-005: Update SCD type 2 ordersummary table on customer table changes.
* FRD-006: Create customeraggregatespend table if not exists.
* FRD-007: Aggregate data from ordersummary table and load into customeraggregatespend table.

#### 2. Title
* Data Loading and Aggregation for Customer and Order Data

#### 3. Description
* The system will load customer and order data from CSV files into Delta tables, remove null and duplicate records, and then join the data to create an ordersummary table. The system will also update the ordersummary table based on changes to the customer table and aggregate data into a customeraggregatespend table.

#### 4. Preconditions
* The system has access to the CSV files containing customer and order data.
* The Delta tables customer and order exist or can be created.
* The catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard" exist or can be created.

#### 5. Main Flow / Functional Steps
* **Step 1: Load Customer and Order Data**
  + Read source CSV data from volume and load to Delta tables customer and order.
  + Customer data is located at: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata
  + Order data is located at: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata
* **Step 2: Remove Null and Duplicate Records**
  + Remove “Null”/Null and duplicate records from both customer and order tables.
* **Step 3: Create ordersummary Table**
  + Create “ordersummary” table if not exists in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
  + Schema of ordersummary table is: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.
* **Step 4: Join Customer and Order Data**
  + Join customer and order data using “CustId” field.
  + Load the joined data into SCD type 2 table under catalog=gen_ai_poc_databrickscoe, schema=sdlc_wizard, table=ordersummary.
* **Step 5: Update SCD Type 2 ordersummary Table**
  + Include logic to update the SCD type 2 table ordersummary whenever there is a change in the customer table.
  + Old records should be made Inactive and new records should be made Active.
  + Update StartDate and EndDate accordingly.
* **Step 6: Create customeraggregatespend Table**
  + Create table if not exists “customeraggregatespend” with columns “Name”, “TotalAmount”, and “Date” in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
* **Step 7: Aggregate Data**
  + Aggregate the “TotalAmount” column from “ordersummary” table and group by “Name” and “Date” columns.
  + Load the aggregated data into “customeraggregatespend” table.

### AFTER FRD GENERATION (CRITICAL)
* After generating and presenting the full FRD to the user (default or custom), you MUST ask exactly:
  "Would you like me to push this FRD to GitHub as a Markdown file from memory?"