# Functional Requirements Document — Application

Here is the detailed Functional Requirement Document (FRD) based on the provided Business Requirement Document (BRD) "Coding Requirements" section:


## Default Output
* In the first response when user provided PDF or DOCX file name, return the short summary(text) of the custom template just to show this is the custom template like 'Your FRD will contain these sections: 1. Requirement ID, 2. Title, 3. Description, 4. Preconditions, 5. Main Flow / Functional Steps' in the chat only (no JSON) and also ask question "Do you want me to use the default FRD template or a custom template you provide?"


## Functional Requirements
### Requirement ID: FRD-001
### Title: Load Data from CSV to Delta Tables
### Description: Load customer and order data from CSV files to Delta tables.
### Preconditions: 
* CSV files are available at the specified locations.
* Delta tables are not existing or can be overwritten.

### Main Flow / Functional Steps:
* Read source CSV data from volume and load to Delta tables customer and order.
* Customer CSV file location: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
* Order CSV file location: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
* Schema of customer table: `CustId, Name, EmailId, Region`
* Schema of order table: `OrderId, ItemName, PricePerUnit, Qty, Date, CustId`

### Requirement ID: FRD-002
### Title: Cleanse Customer and Order Data
### Description: Remove null and duplicate records from customer and order tables.
### Preconditions: 
* Customer and order tables are loaded with data.

### Main Flow / Functional Steps:
* Remove “Null”/Null records from both customer and order tables.
* Remove duplicate records from both customer and order tables.

### Requirement ID: FRD-003
### Title: Create ordersummary Table
### Description: Create ordersummary table if not exists.
### Preconditions: 
* Catalog “gen_ai_poc_databrickscoe” and schema “sdlc_wizard” exist.

### Main Flow / Functional Steps:
* Create “ordersummary” table if not exists in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
* Schema of ordersummary table: `CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date`

### Requirement ID: FRD-004
### Title: Load ordersummary Table
### Description: Join customer and order data and load to ordersummary table.
### Preconditions: 
* Customer and order tables are cleansed.

### Main Flow / Functional Steps:
* Join customer and order data using “CustId” field.
* Load the joined data into SCD type 2 table under catalog=gen_ai_poc_databrickscoe, schema= sdlc_wizard, table=ordersummary.

### Requirement ID: FRD-005
### Title: Update ordersummary Table for Customer Changes
### Description: Update ordersummary table whenever there is a change in customer table.
### Preconditions: 
* ordersummary table is loaded with data.

### Main Flow / Functional Steps:
* Include a logic to update the SCD type 2 table ordersummary whenever there is a change in the customer table.
* Make old records Inactive and new records Active in ordersummary table.
* Update StartDate and EndDate accordingly.

### Requirement ID: FRD-006
### Title: Create customeraggregatespend Table
### Description: Create customeraggregatespend table if not exists.
### Preconditions: 
* Catalog “gen_ai_poc_databrickscoe” and schema “sdlc_wizard” exist.

### Main Flow / Functional Steps:
* Create “customeraggregatespend” table if not exists with columns “Name”, “TotalAmount” and “Date” in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.

### Requirement ID: FRD-007
### Title: Load customeraggregatespend Table
### Description: Aggregate data from ordersummary table and load to customeraggregatespend table.
### Preconditions: 
* ordersummary table is loaded with data.

### Main Flow / Functional Steps:
* Aggregate the “TotalAmount” column from “ordersummary” table and group by “Name” and “Date” columns.
* Load the aggregated data into “customeraggregatespend” table.


## Next Steps
After generating and presenting the full FRD to the user (default or custom), ask: "Would you like me to push this FRD to GitHub as a Markdown file from memory?"