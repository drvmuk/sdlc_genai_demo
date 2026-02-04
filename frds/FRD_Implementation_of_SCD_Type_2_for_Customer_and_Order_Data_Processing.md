# Implementation of SCD Type 2 for Customer and Order Data Processing

### Functional Requirement Document (FRD)

#### 1. Requirement ID
REQ-001

#### 2. Title
Implementation of SCD Type 2 for Customer and Order Data Processing

#### 3. Description
This document outlines the detailed functional requirements for implementing Slowly Changing Dimension (SCD) Type 2 for customer and order data processing. The process involves several key steps including reading source CSV data into delta tables, data cleaning, creating and populating the "ordersummary" table using SCD type 2, updating "ordersummary" on changes to the customer table, and creating "customeraggregatespend" with aggregated data from "ordersummary".

#### 4. Preconditions
- Source CSV files for customer and order data are available and accessible.
- Delta tables for customer and order data exist or can be created in the database.
- Necessary database permissions to create, update, and manage tables.
- The system has the capability to handle SCD Type 2 operations.

#### 5. Main Flow / Functional Steps

1. **Read Source CSV Data into Delta Tables**:
   - The system shall read the customer CSV data into a delta table named "customer_delta".
   - The system shall read the order CSV data into a delta table named "order_delta".

2. **Data Cleaning**:
   - The system shall remove null records from both "customer_delta" and "order_delta" tables.
   - The system shall remove duplicate records from both "customer_delta" and "order_delta" tables based on their primary keys (e.g., "CustId" for customers and "OrderId" for orders).

3. **Create and Populate ordersummary Table**:
   - The system shall create an "ordersummary" table if it does not exist.
   - The system shall join the cleaned "customer_delta" and "order_delta" tables on the "CustId" field.
   - The system shall populate the "ordersummary" table with the joined data.

4. **Implement SCD Type 2 on ordersummary Table**:
   - The system shall implement SCD Type 2 on the "ordersummary" table to track historical changes.
   - Whenever there is a change in the "customer_delta" table, the system shall update the "ordersummary" table accordingly by:
     - Inserting a new record with the updated information.
     - Marking the previous record as inactive or expired by updating the "IsActive" flag or "EndDate".

5. **Create and Populate customeraggregatespend Table**:
   - The system shall create a "customeraggregatespend" table if it does not exist.
   - The system shall aggregate data from the "ordersummary" table (e.g., total spend per customer).
   - The system shall load the aggregated data into the "customeraggregatespend" table.

#### 6. Additional Requirements
- The system shall ensure data consistency and integrity throughout the process.
- The system shall handle errors gracefully and provide logging mechanisms for debugging purposes.

#### 7. Product Owner
- **Name**: [Insert Product Owner Name]
- **Role**: Product Owner
- **Contact Information**: [Insert Contact Information]
- **Responsibilities**:
  - Ensure that the FRD aligns with business requirements.
  - Provide input on the prioritization of requirements.
  - Validate that the implemented solution meets the business needs.
