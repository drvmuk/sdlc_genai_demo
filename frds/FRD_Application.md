# Functional Requirements Document — Application

### Custom 

### OUTPUT
* In the first response when user provided PDF or DOCX file name, return the short summary(text) of the custom template and ask "Do you want me to use the default FRD template or a custom template you provide?"

Let's assume the user has provided a PDF or DOCX file and has chosen to proceed. We will now generate the FRD based on the "Coding Requirements" section of the BRD.

### Functional Requirements

#### Requirement ID: FRD-001
#### Title: Load Customer and Order Data
#### Description: Load customer and order data from CSV files to Delta tables.
#### Preconditions: 
* CSV files for customer and order data are available at the specified locations.
* The schema for customer and order tables is defined.
#### Main Flow / Functional Steps:
* Read source CSV data from the volume and load it to Delta tables customer and order.
* The file paths for customer and order data are `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata` and `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`, respectively.
* The schema for customer and order tables is as follows:
  * customer: CustId, Name, EmailId, Region
  * order: OrderId, ItemName, PricePerUnit, Qty, Date, CustId

#### Requirement ID: FRD-002
#### Title: Clean Customer and Order Data
#### Description: Remove "Null"/Null and duplicate records from customer and order tables.
#### Preconditions: 
* Customer and order data are loaded into Delta tables.
#### Main Flow / Functional Steps:
* Remove "Null"/Null records from both customer and order tables.
* Remove duplicate records from both customer and order tables.

#### Requirement ID: FRD-003
#### Title: Create ordersummary Table
#### Description: Create the ordersummary table if it does not exist.
#### Preconditions: 
* Customer and order tables are cleaned.
#### Main Flow / Functional Steps:
* Create the ordersummary table if it does not exist in the catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard".
* The schema for the ordersummary table is: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.

#### Requirement ID: FRD-004
#### Title: Load ordersummary Table
#### Description: Join customer and order data and load it into the ordersummary table.
#### Preconditions: 
* ordersummary table is created.
#### Main Flow / Functional Steps:
* Join customer and order data using the "CustId" field.
* Load the joined data into the ordersummary table, which is an SCD type 2 table.

#### Requirement ID: FRD-005
#### Title: Update ordersummary Table on Customer Change
#### Description: Update the ordersummary table when there is a change in the customer table.
#### Preconditions: 
* ordersummary table is loaded.
#### Main Flow / Functional Steps:
* When there is a change in the customer table, update the ordersummary table.
* Make old records inactive and new records active.
* Update StartDate and EndDate accordingly.

#### Requirement ID: FRD-006
#### Title: Create customeraggregatespend Table
#### Description: Create the customeraggregatespend table if it does not exist.
#### Preconditions: 
* ordersummary table is updated.
#### Main Flow / Functional Steps:
* Create the customeraggregatespend table if it does not exist in the catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard".
* The schema for the customeraggregatespend table is: Name, TotalAmount, Date.

#### Requirement ID: FRD-007
#### Title: Load customeraggregatespend Table
#### Description: Aggregate data from the ordersummary table and load it into the customeraggregatespend table.
#### Preconditions: 
* customeraggregatespend table is created.
#### Main Flow / Functional Steps:
* Aggregate the TotalAmount column from the ordersummary table and group by Name and Date columns.
* Load the aggregated data into the customeraggregatespend table.

After generating and presenting the full FRD, the next step is:
"Would you like me to push this FRD to GitHub as a Markdown file from memory?"