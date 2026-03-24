**Business Requirements Document (BRD)**
=====================================

**Project Overview**
-------------------

The project involves designing and implementing a data processing pipeline to load, transform, and aggregate data from customer and order datasets. The pipeline will utilize Delta tables and implement Slowly Changing Dimension (SCD) Type 2 logic to maintain historical records.

**Business Requirements**
-------------------------

### 1. Data Ingestion

* Read source CSV data from the specified volumes:
	+ Customer data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
	+ Order data: `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`
* Load the data into Delta tables `customer` and `order`

### 2. Data Schema

* The schema of the `customer` table is: `CustId`, `Name`, `EmailId`, `Region`
* The schema of the `order` table is: `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`, `CustId`

### 3. Data Quality

* Remove null and duplicate records from both `customer` and `order` tables

### 4. Data Transformation and Loading

* Create an `ordersummary` table if it does not exist in the catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard`
* The schema of the `ordersummary` table is: `CustId`, `Name`, `EmailId`, `Region`, `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`
* Join the `customer` and `order` data using the `CustId` field and load the data into an SCD Type 2 table `ordersummary`

### 5. SCD Type 2 Implementation

* Implement SCD Type 2 logic to update the `ordersummary` table whenever there is a change in the `customer` table
* Old records should be marked as inactive, and new records should be marked as active, with corresponding changes to the `StartDate` and `EndDate` fields

### 6. Data Aggregation

* Create a new table `customeraggregatespend` with columns `Name`, `TotalAmount`, and `Date` in the catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard`, if it does not exist
* Aggregate the `TotalAmount` column from the `ordersummary` table and group it by the `Name` and `Date` columns
* Load the aggregated data into the `customeraggregatespend` table

**Functional Requirements**
---------------------------

1. The system shall be able to read CSV data from the specified volumes.
2. The system shall be able to load data into Delta tables `customer` and `order`.
3. The system shall be able to remove null and duplicate records from both tables.
4. The system shall be able to create the `ordersummary` table if it does not exist.
5. The system shall be able to join the `customer` and `order` data and load it into the `ordersummary` table.
6. The system shall implement SCD Type 2 logic to update the `ordersummary` table.
7. The system shall be able to create the `customeraggregatespend` table if it does not exist.
8. The system shall be able to aggregate data from the `ordersummary` table and load it into the `customeraggregatespend` table.

**Non-Functional Requirements**
------------------------------

1. The system shall ensure data consistency and integrity throughout the processing pipeline.
2. The system shall be able to handle large volumes of data efficiently.
3. The system shall be designed to accommodate future changes in the data schema or processing requirements.

**Assumptions and Dependencies**
-------------------------------

1. The input CSV files are in the correct format and are located in the specified volumes.
2. The necessary catalogs and schemas exist in the Databricks environment.
3. The system has the necessary permissions and access rights to read and write data.

**Success Criteria**
--------------------

1. The data is successfully loaded into the Delta tables `customer` and `order`.
2. The data is correctly transformed and loaded into the `ordersummary` table.
3. The SCD Type 2 logic is correctly implemented, and the `ordersummary` table is updated accordingly.
4. The aggregated data is correctly loaded into the `customeraggregatespend` table.

**Testing and Validation**
-------------------------

1. Verify that the data is correctly loaded into the Delta tables.
2. Verify that the data is correctly transformed and loaded into the `ordersummary` table.
3. Verify that the SCD Type 2 logic is correctly implemented.
4. Verify that the aggregated data is correctly loaded into the `customeraggregatespend` table.

By following this BRD, the development team should be able to design and implement a data processing pipeline that meets the business requirements and ensures data consistency and integrity.