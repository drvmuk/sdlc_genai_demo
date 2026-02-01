# Functional Requirements Document — Application

### Functional Requirement Document (FRD)

#### Requirement ID
1. Data Ingestion and Processing

#### Title
Ingest Customer and Order Data, Process, and Generate Aggregate Spend

#### Description
The application will ingest customer and order data from CSV files, process it, and generate aggregate spend data for customers.

#### Preconditions

#### Main Flow / Functional Steps
  • Customer data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata`
  • Order data from `/Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata`

#### Detailed Functional Requirements
  • Customer: CustId, Name, EmailId, Region
  • Order: OrderId, ItemName, PricePerUnit, Qty, Date, CustId