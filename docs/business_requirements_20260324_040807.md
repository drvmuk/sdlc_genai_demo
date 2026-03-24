**Business Requirements Document (BRD)**

**Project Overview**
=====================

The project involves designing and implementing a data processing pipeline to load customer and order data from CSV files into Delta tables, perform data cleansing, and create aggregated summaries.

**Business Requirements**
-------------------------

### 1. Data Ingestion

* Read source CSV data from the volume and load it into the Delta tables "customer" and "order".
* Customer data is located at: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
* Order data is located at: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

### 2. Data Schema

* The schema of the "customer" table is: `CustId`, `Name`, `EmailId`, `Region`
* The schema of the "order" table is: `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`, `CustId`

### 3. Data Cleansing

* Remove "Null"/null and duplicate records from both "customer" and "order" tables.

### 4. Data Transformation and Loading

* Create an "ordersummary" table if it does not exist in the catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard".
* The schema of the "ordersummary" table is: `CustId`, `Name`, `EmailId`, `Region`, `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`.
* Join the "customer" and "order" data using the "CustId" field and load the data into an SCD type 2 table "ordersummary".

### 5. SCD Type 2 Implementation

* Include logic to update the SCD type 2 "ordersummary" table whenever there is a change in the "customer" table.
* In the "ordersummary" table, old records should be made inactive, and new records should be made active.
* Update the `StartDate` and `EndDate` accordingly.

### 6. Aggregated Summary

* Create a new table "customeraggregatespend" with columns "Name", "TotalAmount", and "Date" in catalog "gen_ai_poc_databrickscoe" and schema "sdlc_wizard" if it does not exist.
* Aggregate the "TotalAmount" column from the "ordersummary" table and group it by the "Name" and "Date" columns.
* Load the aggregated data into the "customeraggregatespend" table.

**Functional Requirements**
---------------------------

1. The system shall be able to read CSV data from the specified locations and load it into the Delta tables "customer" and "order".
2. The system shall perform data cleansing on the "customer" and "order" tables by removing "Null"/null and duplicate records.
3. The system shall create the "ordersummary" table if it does not exist and load the joined data from "customer" and "order" tables into it.
4. The system shall implement SCD type 2 logic on the "ordersummary" table to update records when there are changes in the "customer" table.
5. The system shall create the "customeraggregatespend" table if it does not exist and load the aggregated data into it.

**Non-Functional Requirements**
------------------------------

1. The system shall ensure data consistency and integrity throughout the processing pipeline.
2. The system shall be able to handle large volumes of data from the CSV files.

**Assumptions and Dependencies**
-------------------------------

1. The CSV files are in the correct format and are located at the specified paths.
2. The Delta tables "customer", "order", "ordersummary", and "customeraggregatespend" are created in the specified catalog and schema.

**Success Criteria**
--------------------

1. The data is successfully loaded from the CSV files into the Delta tables.
2. The data is cleansed and transformed correctly.
3. The SCD type 2 logic is implemented correctly on the "ordersummary" table.
4. The aggregated data is loaded correctly into the "customeraggregatespend" table.

**Testing Requirements**
------------------------

1. Unit testing shall be performed to ensure that each component of the processing pipeline is working correctly.
2. Integration testing shall be performed to ensure that the entire processing pipeline is working correctly.

By following this BRD, the development team shall be able to design and implement a data processing pipeline that meets the business requirements and ensures data consistency and integrity throughout the processing pipeline.