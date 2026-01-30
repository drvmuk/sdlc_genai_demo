# Functional Requirements Document — Application

Here is the detailed Functional Requirement Document (FRD) based on the provided Business Requirement Document (BRD) and adhering to the specified template and instructions.


## OUTPUT
* In the first response when user provided PDF or DOCX file name, return the short summary(text) of the custom template just to show this is the custom template like 'Your FRD will contain these sections: 1. Requirement ID, 2. Title, 3. Description, 4. Preconditions, 5. Main Flow / Functional Steps' in the chat only (no JSON) and also ask question "Do you want me to use the default FRD template or a custom template you provide?"
* Then:
  - if user said "default" then generate the FRD for the input PDF or DOCX using the Default FRD Template.
  - if user said "custom" then ask user to provide custom template example "Please provide your custom template" and then according to that template, generate FRD for the input PDF or DOCX strictly preserving the user’s template structure, headings, and order.

## Functional Requirements
### Requirement ID: FRD-001
### Title: Data Ingestion and Processing for Customer and Order Data
### Description: The application will read source CSV data, clean and process it, and then load it into Delta tables. It will also create summary tables and aggregate customer spend data.
### Preconditions: 
* The input CSV files for customer and order data are available at the specified volume locations.
* The necessary catalog and schema exist in the Databricks environment.

### Main Flow / Functional Steps:
* **Step 1: Read Source CSV Data**
  * Read customer data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`.
  * Read order data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`.
* **Step 2: Load Data into Delta Tables**
  * Load customer data into a Delta table named "customer" with schema: CustId, Name, EmailId, Region.
  * Load order data into a Delta table named "order" with schema: OrderId, ItemName, PricePerUnit, Qty, Date, CustId.
* **Step 3: Clean Data**
  * Remove "Null"/Null records from both "customer" and "order" tables.
  * Remove duplicate records from both "customer" and "order" tables.
* **Step 4: Create ordersummary Table**
  * Create "ordersummary" table if it does not exist in catalog="gen_ai_poc_databrickscoe" and schema="sdlc_wizard" with schema: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.
* **Step 5: Join and Load Data into ordersummary SCD Type 2 Table**
  * Join "customer" and "order" data on "CustId".
  * Load the joined data into "ordersummary" SCD Type 2 table.
  * Implement logic to update "ordersummary" SCD Type 2 table when there are changes in the "customer" table, making old records Inactive and new records Active, and updating StartDate and EndDate accordingly.
* **Step 6: Create customeraggregatespend Table**
  * Create "customeraggregatespend" table if it does not exist in catalog="gen_ai_poc_databrickscoe" and schema="sdlc_wizard" with columns: "Name", "TotalAmount", and "Date".
* **Step 7: Aggregate and Load Data into customeraggregatespend Table**
  * Aggregate "TotalAmount" from "ordersummary" table, grouping by "Name" and "Date".
  * Load the aggregated data into "customeraggregatespend" table.

### Questions:
* Do you want to proceed with the default FRD template or provide a custom template?
* Are there any specific requirements or changes needed in the outlined functional steps?
