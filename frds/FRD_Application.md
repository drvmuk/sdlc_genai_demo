# Functional Requirements Document — Application

Here is the detailed Functional Requirement Document (FRD) based on the provided Business Requirement Document (BRD) and strictly following the template and "Coding Requirements" section:

**Default**

**OUTPUT**
* In the first response when user provided PDF or DOCX file name, you must return the short summary(text) of the custom template just to show this is the custom template like 'Your FRD will contain these sections: 1. Requirement ID, 2. Title, 3. Description, 4. Preconditions, 5. Main Flow / Functional Steps' in the chat only (no JSON) and also ask question "Do you want me to use the default FRD template or a custom template you provide?" after user gives PDF file.
* Then:

### Functional Requirements

#### Requirement ID: FRD-001
#### Title: Data Ingestion and Processing
#### Description: The system shall ingest data from CSV files, process it, and load it into Delta tables.
#### Preconditions: 
* The CSV files are available at the specified locations.
* The Delta tables are not present or are present with the correct schema.

#### Main Flow / Functional Steps:
* Read source CSV data from the volume and load it into Delta tables "customer" and "order".
  * customer: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata
  * order: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata
* Remove “Null”/Null and Duplicate records from both tables.
* Create “ordersummary” table if not exists in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
* Join customer and order data using “CustId” field and load the data in SCD type 2 table under catalog=gen_ai_poc_databrickscoe, schema= sdlc_wizard, table=ordersummary.
* Include a logic to update the SCD type 2 table ordersummary whenever there is a change in the customer table.
* Create a table if not exists new table “customeraggregatespend” with columns “Name”, “TotalAmount” and “Date” in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
* Aggregate the “TotalAmount” column from “ordersummary” table and group by “Name” and “Date” columns. Load the aggregated data from the “ordersummary” table having columns “Name”, “TotalAmount” and “Date” and load in “customeraggregatespend”.

#### Technical Details:
* The schema of customer and order tables are:
  * customer: CustId, Name, EmailId, Region
  * order: OrderId, ItemName, PricePerUnit, Qty, Date, CustId
* The schema of ordersummary table is: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.
* The SCD type 2 table ordersummary should have the old records made Inactive and new records made Active, and accordingly StartDate and EndDate should be changed.

### AFTER FRD GENERATION (CRITICAL):
* After generating and presenting the full FRD to the user (default or custom), you MUST ask exactly: "Would you like me to push this FRD to GitHub as a Markdown file from memory?"