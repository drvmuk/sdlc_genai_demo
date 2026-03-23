Business Requirements Document (BRD)
Title: Data Pipeline Steps using Delta Live Tables

1. Executive Summary
This BRD defines the business and functional requirements to ingest customer and order CSV data from specified volumes, cleanse and transform the data, create and manage Delta tables, apply SCD Type 2 logic to an ordersummary table, and compute customer daily aggregate spend, all implemented using Delta Live Tables. The document is derived strictly from the provided source JSON. Any information not present in source JSON is explicitly marked.

2. Business Objectives
- Enable ingestion of customer and order data from defined volume paths into Delta tables.
- Standardize schemas for customer and order datasets.
- Enrich order data with computed TotalAmount.
- Improve data quality by removing "Null"/Null values and duplicates.
- Create and maintain an ordersummary SCD Type 2 table to preserve historical changes.
- Produce a customeraggregatespend table with daily total spend per customer.
- Implement the entire pipeline using Delta Live Tables.

3. Scope
In scope:
- Reading CSV source data from defined volume locations into Delta tables: customer, order.
- Data quality operations: removal of "Null"/Null values and duplicates on both tables.
- Transformations: compute TotalAmount = PricePerUnit × Qty in order table.
- Dimensional integration: join customer and order on CustId.
- SCD Type 2 implementation on ordersummary table with StartDate, EndDate, Active/Inactive status, and full history maintenance.
- Aggregation: grouping by Name and Date to compute TotalAmount daily totals into customeraggregatespend.
- Creation of tables under catalog gen_ai_poc_databrickscoe and schema sdlc_wizard where specified.
- Implementation using Delta Live Tables.

Out of scope:
- Information not present in source JSON.

4. Stakeholders
- Information not present in source JSON.

5. Current State Overview
- Information not present in source JSON.

6. Future State Overview
- A Delta Live Tables pipeline ingests CSV data from specified volumes, standardizes schemas, applies data quality rules, enriches orders with TotalAmount, builds and maintains an SCD Type 2 ordersummary table, and produces a customeraggregatespend table by Name and Date, all within catalog gen_ai_poc_databrickscoe and schema sdlc_wizard.

7. Functional Requirements
FR-1 Data Ingestion
- Read source CSV data from volumes into Delta tables:
  - customer from /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata
  - order from /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata

FR-2 Schema Enforcement
- Apply the following schemas:
  - customer: CustId, Name, EmailId, Region
  - order: OrderId, ItemName, PricePerUnit, Qty, Date, CustId

FR-3 Derived Column
- In order table, add column TotalAmount = PricePerUnit × Qty. Expression not provided in source JSON beyond the stated formula.

FR-4 Data Quality
- Remove "Null"/Null values and duplicate records from both customer and order tables. Specific column rules not provided in source JSON.

FR-5 Table Creation: ordersummary
- Ensure ordersummary exists under:
  - catalog: gen_ai_poc_databrickscoe
  - schema: sdlc_wizard
- ordersummary schema fields provided: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.
- Note: StartDate, EndDate, and status columns implied by SCD Type 2 are not enumerated in the provided ordersummary schema.

FR-6 Join and Load to SCD Type 2
- Join customer and order on CustId and load results into SCD Type 2 table ordersummary at:
  - catalog: gen_ai_poc_databrickscoe
  - schema: sdlc_wizard
  - table: ordersummary

FR-7 SCD Type 2 Behavior
- When customer data changes, mark old records as Inactive and new records as Active.
- Update StartDate and EndDate accordingly.
- Maintain full history in ordersummary.

FR-8 Table Creation: customeraggregatespend
- Ensure customeraggregatespend exists with columns: Name, TotalAmount, Date under:
  - catalog: gen_ai_poc_databrickscoe
  - schema: sdlc_wizard

FR-9 Aggregation
- Aggregate TotalAmount from ordersummary grouped by Name and Date.
- Load results into customeraggregatespend with columns: Name, TotalAmount, Date.
- Aggregation expression (SUM/other) not specified in source JSON. Aggregation expression must be declared or marked missing: Expression not provided in source JSON.

FR-10 Implementation Technology
- Implement steps 1–10 using Delta Live Tables.

8. Non-Functional Requirements
- Information not present in source JSON.

9. Business Rules
BR-1 Null and Duplicate Handling
- Remove "Null"/Null values and duplicates from both customer and order tables. Columns and deduplication keys not specified in source JSON.

BR-2 TotalAmount Calculation
- TotalAmount = PricePerUnit × Qty in order.

BR-3 SCD Type 2
- On customer data change:
  - Prior record: Inactive; new record: Active.
  - Update StartDate and EndDate.
  - Maintain full history.
- Exact SCD comparison fields and change detection rules not specified in source JSON.

BR-4 Aggregation
- Group by Name, Date from ordersummary to create customeraggregatespend with TotalAmount aggregated. Aggregation function not explicitly specified; derivation implies summation but per rule, Expression not provided in source JSON.

10. Data and Reporting Requirements
- Source datasets: customer, order.
- Integrated dataset: ordersummary (SCD Type 2).
- Aggregate dataset: customeraggregatespend by Name and Date.
- Reporting or consumption endpoints: Information not present in source JSON.

11. Assumptions and Constraints
- Assumptions: None beyond explicit JSON content.
- Constraints:
  - Use Delta Live Tables for pipeline implementation.
  - Catalog and schema for target tables: gen_ai_poc_databrickscoe.sdlc_wizard where specified.
  - No external fields or logic beyond what is provided.

12. Dependencies and Lineage
- Upstream sources: CSV files at given volume paths.
- Transformations: TotalAmount computation; null/duplicate removal; join on CustId; SCD Type 2 application; aggregation to customeraggregatespend.
- Load order dependency:
  1) Read customer and order
  2) Cleanse data
  3) Compute TotalAmount in order
  4) Create ordersummary if not exists
  5) Join customer and order => ordersummary (SCD2)
  6) Create customeraggregatespend if not exists
  7) Aggregate from ordersummary => customeraggregatespend
- Name mismatches between lineage pairs: Not detected from source JSON.

13. Risks and Mitigations
- Risk: Ambiguity in aggregation function for TotalAmount. Mitigation: Specify SUM in design or confirm with stakeholders.
- Risk: SCD Type 2 requires StartDate/EndDate/Active columns not listed in ordersummary schema. Mitigation: Extend schema explicitly in design approval.
- Risk: Null and duplicate removal criteria unspecified. Mitigation: Define column-level rules before implementation.

14. Implementation Approach and Environments
- Technology: Delta Live Tables.
- Environments: Information not present in source JSON.

15. Glossary
- Delta Live Tables: Not present in source JSON.
- SCD Type 2: Not present in source JSON.
- Other terms: Not present in source JSON.

Appendix A: Repository and Context Metadata
| Field | Value |
|---|---|
| Title | Data Pipeline Steps using Delta Live Tables |
| Implementation Tool | Delta Live Tables |
| Notes | All requirements derived strictly from source JSON. Missing details explicitly marked. |

Appendix B: Source Definitions
| Source Name | Type | Path/Location | Format | Schema Fields |
|---|---|---|---|---|
| customer | Volume | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata | Not present in source JSON | CustId; Name; EmailId; Region |
| order | Volume | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata | Not present in source JSON | OrderId; ItemName; PricePerUnit; Qty; Date; CustId |

Appendix C: Target Definitions
| Target Name | Catalog | Schema | Table | Columns |
|---|---|---|---|---|
| ordersummary | gen_ai_poc_databrickscoe | sdlc_wizard | ordersummary | CustId; Name; EmailId; Region; OrderId; ItemName; PricePerUnit; Qty; Date |
| customeraggregatespend | gen_ai_poc_databrickscoe | sdlc_wizard | Not present in source JSON | Name; TotalAmount; Date |

Appendix D: Inventory of Tables and Attributes
| Table | Attribute | Data Type | Constraints/Notes |
|---|---|---|---|
| customer | CustId | Not present in source JSON | Not present in source JSON |
| customer | Name | Not present in source JSON | Not present in source JSON |
| customer | EmailId | Not present in source JSON | Not present in source JSON |
| customer | Region | Not present in source JSON | Not present in source JSON |
| order | OrderId | Not present in source JSON | Not present in source JSON |
| order | ItemName | Not present in source JSON | Not present in source JSON |
| order | PricePerUnit | Not present in source JSON | Not present in source JSON |
| order | Qty | Not present in source JSON | Not present in source JSON |
| order | Date | Not present in source JSON | Not present in source JSON |
| order | CustId | Not present in source JSON | Not present in source JSON |
| ordersummary | CustId | Not present in source JSON | From join on CustId |
| ordersummary | Name | Not present in source JSON | From customer |
| ordersummary | EmailId | Not present in source JSON | From customer |
| ordersummary | Region | Not present in source JSON | From customer |
| ordersummary | OrderId | Not present in source JSON | From order |
| ordersummary | ItemName | Not present in source JSON | From order |
| ordersummary | PricePerUnit | Not present in source JSON | From order |
| ordersummary | Qty | Not present in source JSON | From order |
| ordersummary | Date | Not present in source JSON | From order |
| customeraggregatespend | Name | Not present in source JSON | From ordersummary |
| customeraggregatespend | TotalAmount | Not present in source JSON | Aggregated from ordersummary |
| customeraggregatespend | Date | Not present in source JSON | From ordersummary |

Appendix E: Transformations
| Step | Source | Target | Transformation Description | Expression |
|---|---|---|---|---|
| T1 | order | order (staged) | Add TotalAmount column | TotalAmount = PricePerUnit × Qty (Expression not provided in source JSON) |
| T2 | customer, order | ordersummary | Join on CustId; load to SCD Type 2 table | Join condition: customer.CustId = order.CustId (Expression not provided in source JSON) |
| T3 | ordersummary | customeraggregatespend | Aggregate TotalAmount grouped by Name, Date | Aggregation expression not provided in source JSON |

Appendix F: Data Quality Rules
| Rule ID | Table | Rule Type | Description | Expression/Criteria |
|---|---|---|---|---|
| DQ-1 | customer | Null Handling | Remove "Null"/Null values | Expression not provided in source JSON |
| DQ-2 | customer | Duplicate Handling | Remove duplicate records | Expression not provided in source JSON |
| DQ-3 | order | Null Handling | Remove "Null"/Null values | Expression not provided in source JSON |
| DQ-4 | order | Duplicate Handling | Remove duplicate records | Expression not provided in source JSON |

Appendix G: SCD Type 2 Specifications
| Table | Business Key | Change Detection Fields | StartDate Column | EndDate Column | Current Flag | Notes |
|---|---|---|---|---|---|---|
| ordersummary | Not present in source JSON | Not present in source JSON | StartDate (implied) | EndDate (implied) | Active/Inactive (status) | Maintain full history; mark old as Inactive, new as Active. Columns not included in listed schema. |

Appendix H: Parameters and Variables
| Name | Scope | Default/Value | Description |
|---|---|---|---|
| Not present in source JSON | Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix I: Configuration Flags
| Component | Flag | Value |
|---|---|---|
| Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix J: Connectivity and Ports
| System/Component | Direction | Protocol/Port | Details |
|---|---|---|---|
| Not present in source JSON | Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix K: Lineage
| Order | From | To | Operation |
|---|---|---|---|
| 1 | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata | customer (Delta) | Read CSV into Delta |
| 2 | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata | order (Delta) | Read CSV into Delta |
| 3 | order (Delta) | order (enriched) | Compute TotalAmount |
| 4 | customer, order (enriched) | ordersummary | Join on CustId; SCD Type 2 load |
| 5 | ordersummary | customeraggregatespend | Aggregate by Name, Date |

Appendix L: Flow and Load Order
| Step | Description |
|---|---|
| 1 | Ingest customer CSV and order CSV into Delta tables |
| 2 | Remove "Null"/Nulls and duplicates from both tables |
| 3 | Compute TotalAmount in order |
| 4 | Create ordersummary if not exists (catalog/schema provided) |
| 5 | Join customer and order on CustId and load to ordersummary with SCD Type 2 |
| 6 | Create customeraggregatespend if not exists (catalog/schema provided) |
| 7 | Aggregate from ordersummary to customeraggregatespend by Name and Date |

Appendix M: Environments
| Environment | Details |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix N: Security and Access
| Area | Requirement |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix O: Testing Approach
| Test Type | Scope |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix P: Acceptance Criteria
| ID | Criterion |
|---|---|
| AC-1 | Pipeline implemented using Delta Live Tables across steps 1–10 |
| AC-2 | ordersummary exists in gen_ai_poc_databrickscoe.sdlc_wizard with listed columns |
| AC-3 | customeraggregatespend exists in gen_ai_poc_databrickscoe.sdlc_wizard with columns Name, TotalAmount, Date |
| AC-4 | SCD Type 2 behavior: history maintained; StartDate/EndDate updated; Active/Inactive status set |
| AC-5 | "Null"/Null values and duplicates removed from both source tables |
| AC-6 | TotalAmount computed as PricePerUnit × Qty |

Appendix Q: Open Questions
| ID | Question |
|---|---|
| Q1 | Confirm aggregation function for TotalAmount in customeraggregatespend (assumed SUM?). |
| Q2 | Specify data types and nullable constraints for all columns. |
| Q3 | Define deduplication keys and null-removal criteria per column. |
| Q4 | Confirm inclusion and names of SCD columns (StartDate, EndDate, Active/Inactive) in ordersummary schema. |
| Q5 | Confirm CSV formats (delimiter, header, encoding) and any schema evolution handling. |
| Q6 | Define business key for SCD Type 2 in ordersummary. |
| Q7 | Confirm whether customeraggregatespend table name is precisely “customeraggregatespend” (table property not explicitly provided). |

Appendix R: Traceability Matrix
| Requirement ID | Source JSON Reference | Notes |
|---|---|---|
| FR-1 | "1. Read source CSV data from volumes and load into Delta tables: customer and order." | Paths provided |
| FR-2 | "2. Schemas:" | Field names provided |
| FR-3 | "3. In the order table, add a column TotalAmount = PricePerUnit × Qty." | Formula stated |
| FR-4 | "4. Remove 'Null'/Null values and duplicate records from both tables." | True flag |
| FR-5 | "5. Create the ordersummary table if it does not exist:" | Catalog/schema provided |
| FR-6 | "6. Join customer and order on CustId and load the results into an SCD Type 2 table:" | Catalog/schema/table provided |
| FR-7 | "8. Implement SCD Type 2 logic on ordersummary:" | Behavior flags true |
| FR-8 | "9. Create the customeraggregatespend table if it does not exist:" | Columns and catalog/schema provided |
| FR-9 | "10. Aggregate TotalAmount from ordersummary grouped by Name and Date..." | True flag; expression missing |
| FR-10 | "11. Implement steps 1–10 using Delta Live Tables." | True flag |

Appendix S: Exceptions and Limitations
| Area | Limitation |
|---|---|
| Aggregations | Aggregation expression for TotalAmount not provided. |
| SCD | SCD columns not included in listed ordersummary schema. |
| Data Types | Not provided. |
| Operational Details | Scheduling, SLAs, monitoring not provided. |

Appendix T: Additional Notes
| Note |
|---|
| All unspecified details require confirmation prior to implementation to avoid misinterpretation. |