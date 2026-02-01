# Functional Requirements Document — Application

### 1. Requirement ID

### 2. Title

### 3. Description

### 4. Preconditions

### 5. Main Flow / Functional Steps
	+ Read customer data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`.
	+ Read order data from CSV file located at `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`.
	+ Load the data into Delta tables `customer` and `order` respectively.
	+ Remove null and duplicate records from both `customer` and `order` tables.
	+ Create a table `ordersummary` if it does not exist in catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard`.
	+ The schema of `ordersummary` table is: `CustId`, `Name`, `EmailId`, `Region`, `OrderId`, `ItemName`, `PricePerUnit`, `Qty`, `Date`.
	+ Join `customer` and `order` data using `CustId` field.
	+ Load the joined data into `ordersummary` table, which is an SCD type 2 table.
	+ Include logic to update `ordersummary` table whenever there is a change in `customer` table.
	+ Make old records inactive and new records active, and update `StartDate` and `EndDate` accordingly.
	+ Create a table `customeraggregatespend` if it does not exist in catalog `gen_ai_poc_databrickscoe` and schema `sdlc_wizard`.
	+ The schema of `customeraggregatespend` table is: `Name`, `TotalAmount`, `Date`.
	+ Aggregate `TotalAmount` from `ordersummary` table and group by `Name` and `Date` columns.
	+ Load the aggregated data into `customeraggregatespend` table.
