# # Flow Diagram

1. Requirement Overview
The requirement is to develop a data processing pipeline that reads customer and order data from CSV files, transforms the data, and loads it into various tables in a Databricks catalog.

2. Actors and Roles
* The system will be acting as the primary actor to process the customer and order data.

3. Functional Scope
* Read customer and order data from CSV files.
* Load data into delta tables.
* Remove null and duplicate records.
* Create and populate the "ordersummary" table using SCD type 2.
* Update the "ordersummary" table when there are changes in the customer table.
* Create and populate the "customeraggregatespend" table.

4. Main Flow
* Read source CSV data from volume and load to delta tables "customer" and "order".
  - Customer data path: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata
  - Order data path: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata
* Remove "Null"/Null and Duplicate records from both "customer" and "order" tables.
* Create "ordersummary" table if not exists in catalog="gen_ai_poc_databrickscoe" and schema="sdlc_wizard".
* Join "customer" and "order" data using "CustId" field and load the data into SCD type 2 "ordersummary" table.
* Include logic to update the SCD type 2 "ordersummary" table whenever there is a change in the "customer" table.
* Create "customeraggregatespend" table if not exists with columns "Name", "TotalAmount", and "Date".
* Aggregate "TotalAmount" from "ordersummary" table and group by "Name" and "Date" columns.
* Load the aggregated data into "customeraggregatespend" table.

5. Alternate / Exception Flows
* Handling null and duplicate records in "customer" and "order" tables.
* Updating SCD type 2 "ordersummary" table when changes occur in the "customer" table.

6. Preconditions
* Customer and order CSV files exist at the specified paths.
* Databricks catalog and schema are properly configured.

7. Postconditions
* "ordersummary" table is populated with joined customer and order data.
* "customeraggregatespend" table is populated with aggregated data.
* SCD type 2 "ordersummary" table is updated when changes occur in the "customer" table.

8. Validation Rules
* "customer" table schema: CustId, Name, EmailId, Region.
* "order" table schema: OrderId, ItemName, PricePerUnit, Qty, Date, CustId.
* "ordersummary" table schema: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.
* "customeraggregatespend" table schema: Name, TotalAmount, Date.
* TotalAmount is calculated by aggregating data from "ordersummary" table.

## Flow Diagram

```mermaid
flowchart TD
    A[Read customer and order CSV data] --> B[Load data into delta tables customer and order]
    B --> C[Remove null and duplicate records from customer and order tables]
    C --> D[Create ordersummary table if not exists]
    D --> E[Join customer and order data and load into SCD type 2 ordersummary table]
    E --> F[Update SCD type 2 ordersummary table when changes occur in customer table]
    F --> G[Create customeraggregatespend table if not exists]
    G --> H[Aggregate TotalAmount from ordersummary table]
    H --> I[Load aggregated data into customeraggregatespend table]
```

Flow Diagram section added with PNG image!


## Flow Diagram

```mermaid
flowchart TD
A[Read CSV Data] -->|Customer Data| B[Load into Customer Delta Table]
A -->|Order Data| C[Load into Order Delta Table]
B --> D[Remove Null and Duplicate Records from Customer Table]
C --> E[Remove Null and Duplicate Records from Order Table]
D --> F[Join Customer and Order Data]
E --> F
F --> G[Load into ordersummary SCD Type 2 Table]
G --> H[Update ordersummary on Customer Data Change]
H --> I[Create customeraggregatespend Table]
I --> J[Load Aggregated Customer Spend Data]
```

Flow Diagram section added with PNG image (rendered as Mermaid diagram above).
