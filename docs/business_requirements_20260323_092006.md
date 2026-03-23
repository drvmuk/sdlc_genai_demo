Business Requirements Document (BRD)
Title: Data Processing Steps for Customer and Order Pipelines (Delta Live Tables)

1. Executive Summary
- Purpose: Define business and functional requirements to ingest, cleanse, transform, and persist customer and order data using Delta Live Tables, produce an SCD Type 2 ordersummary, and an aggregated customeraggregatespend table.
- Outcome: Reliable Delta tables for customer, order, ordersummary (SCD2), and customeraggregatespend with defined schemas, data quality rules, transformations, and aggregations.
- Scope: Reading CSV sources, schema definitions, transformations, deduplication and null removal, SCD Type 2 implementation, aggregation, and table creation in the specified catalog/schema.

2. Business Objectives
- Enable standardized ingestion from CSV sources to Delta format.
- Maintain historical changes via SCD Type 2 for order and customer attributes in ordersummary.
- Provide aggregate spend per customer per date in customeraggregatespend.
- Ensure data quality through null removal and deduplication.

3. In-Scope
- Reading customer and order CSV data from specified volume paths into Delta tables.
- Schema definitions for customer, order, and ordersummary.
- Transformation to compute TotalAmount for orders.
- Data quality steps: null removal and deduplication for customer and order.
- Creation and maintenance of ordersummary as SCD Type 2.
- Aggregation from ordersummary to customeraggregatespend.
- Use of Delta Live Tables to orchestrate steps 1–10.

Out of Scope
- Information not present in source JSON.

4. Stakeholders and RACI
- Stakeholders: Information not present in source JSON.
- RACI:
  - Responsible: Information not present in source JSON.
  - Accountable: Information not present in source JSON.
  - Consulted: Information not present in source JSON.
  - Informed: Information not present in source JSON.

5. Current State Assessment
- Information not present in source JSON.

6. Future State Overview
- A Delta Live Tables pipeline ingests CSVs into Delta tables, applies schema enforcement, quality rules, joins customer and order to build an SCD Type 2 ordersummary with full history, and aggregates spend into customeraggregatespend for reporting.

7. Detailed Business Requirements
BR-1 Source Ingestion
- Read customer and order data from provided volume paths in CSV format and load into Delta tables.

BR-2 Schema Enforcement
- Enforce explicit schemas for customer and order as provided. Enforce defined schema for ordersummary.

BR-3 Transformation: TotalAmount
- Derive TotalAmount on order as PricePerUnit * Qty.

BR-4 Data Quality
- Remove Null values and nulls; deduplicate records for customer and order tables.

BR-5 Ordersummary Table Creation
- Create ordersummary table if it does not exist in catalog gen_ai_poc_databrickscoe and schema sdlc_wizard.

BR-6 Join and Load to SCD2
- Inner join customer and order on CustId; load result into ordersummary as SCD Type 2.

BR-7 SCD Type 2 Tracking
- Implement SCD2 with business keys CustId and OrderId, status column RecordStatus with values Active/Inactive, validity columns StartDate/EndDate, and rules to maintain full history.

BR-8 Aggregation to customeraggregatespend
- Aggregate sum(TotalAmount) from ordersummary grouped by Name and Date into customeraggregatespend with target columns Name, TotalAmount, Date. Create table if not exists in specified catalog/schema.

BR-9 Technology Enablement
- Implement steps 1–10 using Delta Live Tables.

8. Functional Requirements
FR-1 Source Definitions
- Sources: customer (CSV), order (CSV) at specified paths. Load targets in Delta format.

FR-2 Table Schemas
- customer: CustId, Name, EmailId, Region.
- order: OrderId, ItemName, PricePerUnit, Qty, Date, CustId.
- ordersummary: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date. (SCD2 columns RecordStatus, StartDate, EndDate also referenced in SCD2 rules.)

FR-3 Transformations
- Add column TotalAmount to order with expression PricePerUnit * Qty.

FR-4 Data Quality
- Remove values: "Null" and null. Deduplicate: true for customer and order.

FR-5 Joins
- Inner join on CustId between customer (left) and order (right).

FR-6 SCD2 Logic
- Business keys: CustId, OrderId.
- Change tracking source: customer.
- Status: RecordStatus with Active/Inactive.
- Validity: StartDate, EndDate.
- Rules per Section 9.

FR-7 Aggregations
- From ordersummary group by Name, Date; measure sum(TotalAmount); load to customeraggregatespend.

FR-8 Orchestration
- Utilize Delta Live Tables to implement steps 1–10.

9. Business Rules
- BRU-1 Null Handling: Remove rows containing "Null" (string) or null values for customer and order. Specific columns not provided in source JSON.
- BRU-2 Deduplication: Remove duplicate records from customer and order. Deduplication keys not provided in source JSON.
- BRU-3 Computation: TotalAmount = PricePerUnit * Qty.
- BRU-4 Join: Inner join on CustId between customer and order.
- BRU-5 SCD2 Status: When customer attributes change, mark previous ordersummary records as Inactive and new as Active; update StartDate/EndDate accordingly; maintain full history.
- BRU-6 Aggregation: For each Name and Date, TotalAmount = sum(TotalAmount) from ordersummary.

10. Non-Functional Requirements
- Performance: Information not present in source JSON.
- Scalability: Information not present in source JSON.
- Availability: Information not present in source JSON.
- Security and Access: Information not present in source JSON.
- Audit/Lineage: Captured via steps and Appendices; additional details not present in source JSON.
- Compliance: Information not present in source JSON.

11. Assumptions and Constraints
- Assumptions: None beyond explicit JSON content.
- Constraints: Use Delta Live Tables for implementation; target catalog/schema as specified; CSV as source format; Delta as target format.

12. Dependencies and Data Lineage
- Dependencies derived from step order and joins:
  - Step 1: Sources customer and order required before downstream steps.
  - Step 3 depends on Step 2 for schema and Step 1 ingestion of order.
  - Step 4 depends on Step 1/2 for existing tables.
  - Step 5 creates ordersummary before load (Step 6).
  - Step 6 join depends on customer and order; loads to ordersummary.
  - Step 7 defines ordersummary schema; used by SCD2 logic (Step 8).
  - Step 8 depends on ordersummary and change_tracking_source customer.
  - Step 9 creates customeraggregatespend before aggregation load (Step 10).
  - Step 10 depends on ordersummary.
  - Step 11 implements Steps 1–10 using Delta Live Tables.
- Name mismatches between lineage pairs: Not identified based on provided names.

13. Risks and Mitigations
- Risk: Ambiguity in deduplication keys. Mitigation: Define keys in implementation design. Information not present in source JSON.
- Risk: Null removal scope unclear. Mitigation: Specify columns/thresholds in technical design. Information not present in source JSON.
- Risk: SCD2 additional columns presence. Mitigation: Ensure RecordStatus, StartDate, EndDate are present in ordersummary physical schema.

14. Implementation Plan and Milestones
- Milestones:
  - M1: Define Delta Live Tables pipeline with steps 1–10.
  - M2: Implement schemas and ingestion.
  - M3: Implement transformations and data quality.
  - M4: Implement join and SCD2 logic.
  - M5: Implement aggregation and target loads.
  - M6: Validate outputs.
- Dates/owners: Information not present in source JSON.

15. Glossary
- SCD Type 2: Slowly Changing Dimension methodology that preserves historical records by closing out prior versions and creating new active versions.
- Delta Live Tables: Technology referenced for pipeline orchestration.
- Business Keys: Identifiers used to track SCD2 versions (CustId, OrderId).
- Not present in source JSON for any additional terms.

Appendix A: Repository and Technology Metadata
| Field | Value |
|---|---|
| Title | Data Processing Steps for Customer and Order Pipelines (Delta Live Tables) |
| Technology | Delta Live Tables |
| Catalog | gen_ai_poc_databrickscoe |
| Schema | sdlc_wizard |

Appendix B: Source Definitions and Inventory
| Source Name | Path | Format | Notes |
|---|---|---|---|
| customer | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata | csv | Not present in source JSON for additional details |
| order | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata | csv | Not present in source JSON for additional details |

Appendix C: Target Definitions and Inventory
| Target Table | Catalog | Schema | Format | If Not Exists |
|---|---|---|---|---|
| customer | Not present in source JSON | Not present in source JSON | delta | Not present in source JSON |
| order | Not present in source JSON | Not present in source JSON | delta | Not present in source JSON |
| ordersummary | gen_ai_poc_databrickscoe | sdlc_wizard | Not present in source JSON | true |
| customeraggregatespend | gen_ai_poc_databrickscoe | sdlc_wizard | Not present in source JSON | true |

Appendix D: Table Schemas
| Table | Column | Data Type | Notes |
|---|---|---|---|
| customer | CustId | Not present in source JSON | From source JSON |
| customer | Name | Not present in source JSON | From source JSON |
| customer | EmailId | Not present in source JSON | From source JSON |
| customer | Region | Not present in source JSON | From source JSON |
| order | OrderId | Not present in source JSON | From source JSON |
| order | ItemName | Not present in source JSON | From source JSON |
| order | PricePerUnit | Not present in source JSON | From source JSON |
| order | Qty | Not present in source JSON | From source JSON |
| order | Date | Not present in source JSON | From source JSON |
| order | CustId | Not present in source JSON | From source JSON |
| ordersummary | CustId | Not present in source JSON | From source JSON |
| ordersummary | Name | Not present in source JSON | From source JSON |
| ordersummary | EmailId | Not present in source JSON | From source JSON |
| ordersummary | Region | Not present in source JSON | From source JSON |
| ordersummary | OrderId | Not present in source JSON | From source JSON |
| ordersummary | ItemName | Not present in source JSON | From source JSON |
| ordersummary | PricePerUnit | Not present in source JSON | From source JSON |
| ordersummary | Qty | Not present in source JSON | From source JSON |
| ordersummary | Date | Not present in source JSON | From source JSON |
| ordersummary | RecordStatus | Not present in source JSON | Referenced in SCD2 rules |
| ordersummary | StartDate | Not present in source JSON | Referenced in SCD2 rules |
| ordersummary | EndDate | Not present in source JSON | Referenced in SCD2 rules |
| customeraggregatespend | Name | Not present in source JSON | From source JSON |
| customeraggregatespend | TotalAmount | Not present in source JSON | From source JSON |
| customeraggregatespend | Date | Not present in source JSON | From source JSON |

Appendix E: Transformations
| Table | Operation | Column | Expression | Notes |
|---|---|---|---|---|
| order | add_columns | TotalAmount | PricePerUnit * Qty | From source JSON |

Appendix F: Data Quality and Cleansing Rules
| Rule Type | Tables | Details |
|---|---|---|
| Remove Values | customer, order | Values to remove: "Null", null |
| Deduplication | customer, order | deduplicate = true |

Appendix G: Joins and Relationships
| Left Table | Right Table | Join Keys | Join Type | Target |
|---|---|---|---|---|
| customer | order | CustId | inner | ordersummary |

Appendix H: SCD Type 2 Configuration
| Table | Business Keys | Change Tracking Source | Status Column | Status Active | Status Inactive | Start Column | End Column | Rules |
|---|---|---|---|---|---|---|---|---|
| ordersummary | CustId, OrderId | customer | RecordStatus | Active | Inactive | StartDate | EndDate | When customer attributes change, mark old records as Inactive and new records as Active; Update StartDate and EndDate accordingly; Maintain full history in the table |

Appendix I: Aggregations and Measures
| Source Table | Group By | Measure Name | Measure Expression | Target Table | Target Columns |
|---|---|---|---|---|---|
| ordersummary | Name, Date | TotalAmount | sum(TotalAmount) | customeraggregatespend | Name, TotalAmount, Date |

Appendix J: Pipeline Flow, Steps, and Load Order
| Step | Description | Depends On | Output/Target |
|---|---|---|---|
| 1 | Read source CSV data from volumes and load into Delta tables | None | Delta tables for customer and order |
| 2 | Define schemas | 1 | Enforced schemas for customer and order |
| 3 | Add TotalAmount column to order | 1,2 | order with TotalAmount |
| 4 | Remove Null values and duplicates from both tables | 1–3 | Cleansed customer and order |
| 5 | Create the ordersummary table if it does not exist | 1–4 | ordersummary (empty or existing) |
| 6 | Join customer and order on CustId and load into SCD Type 2 table ordersummary | 1–5 | ordersummary populated |
| 7 | Define ordersummary schema | 5–6 | Enforced ordersummary schema |
| 8 | Implement SCD Type 2 logic for ordersummary | 6–7 | Historical tracking in ordersummary |
| 9 | Create the customeraggregatespend table if it does not exist | 6–8 | customeraggregatespend (empty or existing) |
| 10 | Aggregate TotalAmount grouped by Name and Date into customeraggregatespend | 6–9 | customeraggregatespend populated |
| 11 | Implement steps 1–10 using Delta Live Tables | 1–10 | Orchestrated pipeline |

Appendix K: Parameters and Variables
| Name | Value | Scope | Notes |
|---|---|---|---|
| target_format | delta | Step 1 | From source JSON |
| scd_type | 2 | Step 6 | From source JSON |
| if_not_exists (ordersummary) | true | Step 5 | From source JSON |
| if_not_exists (customeraggregatespend) | true | Step 9 | From source JSON |

Appendix L: Configuration Flags
| Config | Value | Applies To |
|---|---|---|
| deduplicate | true | Data quality for customer and order |
| remove_values | "Null", null | Data quality for customer and order |

Appendix M: Environment and Deployment
| Environment | Details |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix N: Error Handling and Logging
| Aspect | Details |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix O: Security, Access Control, and Governance
| Aspect | Details |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix P: Data Lineage Matrix
| Source | Operation | Intermediate/Target | Notes |
|---|---|---|---|
| customer (CSV) | Ingest to Delta | customer (Delta) | Step 1 |
| order (CSV) | Ingest to Delta | order (Delta) | Step 1 |
| order | Add column | order.TotalAmount | Step 3 |
| customer, order | Inner join on CustId | ordersummary | Step 6 |
| ordersummary | SCD2 versioning | ordersummary (history) | Step 8 |
| ordersummary | Aggregate sum(TotalAmount) by Name, Date | customeraggregatespend | Step 10 |

Appendix Q: Naming Conventions and Mismatch Log
| Entity A | Entity B | Mismatch Type | Notes |
|---|---|---|---|
| Not present in source JSON | Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix R: Scheduling and Orchestration
| Orchestrator | Schedule | Notes |
|---|---|---|
| Delta Live Tables | Not present in source JSON | Implements steps 1–10 |

Appendix S: Testing and Validation Scenarios
| Test ID | Objective | Input | Expected Output |
|---|---|---|---|
| T1 | Validate TotalAmount calculation | order with PricePerUnit and Qty | TotalAmount = PricePerUnit * Qty |
| T2 | Validate null removal | Records with "Null"/null | Records removed per rule |
| T3 | Validate deduplication | Duplicate customer/order rows | Duplicates removed |
| T4 | Validate join correctness | Matching CustId | Correct inner join result in ordersummary |
| T5 | Validate SCD2 behavior | Change in customer attributes | Prior record Inactive, new Active, dates updated |
| T6 | Validate aggregation | ordersummary with TotalAmount | Aggregated sums by Name, Date in customeraggregatespend |

Appendix T: Open Questions
| ID | Question | Status |
|---|---|---|
| Q1 | What are data types for each column? | Not present in source JSON |
| Q2 | Which columns define deduplication uniqueness? | Not present in source JSON |
| Q3 | Environments (dev/test/prod) and deployment details? | Not present in source JSON |
| Q4 | Security and access controls for catalog/schema? | Not present in source JSON |
| Q5 | Scheduling cadence and SLAs? | Not present in source JSON |

Appendix U: Constraints and Limits
| Constraint | Detail |
|---|---|
| Source format | csv |
| Target format | delta |
| Technology | Delta Live Tables |

Appendix V: Expressions and Calculations
| Expression Name | Table | Expression | Description |
|---|---|---|---|
| TotalAmount Calculation | order | PricePerUnit * Qty | Computes order line extended amount |
| Aggregation Measure | ordersummary | sum(TotalAmount) | Aggregates spend per customer per date |

Appendix W: Validation Columns and Status Flags
| Table | Column | Purpose | Allowed Values |
|---|---|---|---|
| ordersummary | RecordStatus | SCD2 active/inactive flag | Active, Inactive |
| ordersummary | StartDate | SCD2 validity start | Not present in source JSON |
| ordersummary | EndDate | SCD2 validity end | Not present in source JSON |

Appendix X: Performance and Scaling
| Topic | Detail |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix Y: Compliance and Regulatory
| Topic | Detail |
|---|---|
| Not present in source JSON | Not present in source JSON |

Appendix Z: Change Log
| Version | Date | Description | Author |
|---|---|---|---|
| 1.0 | Not present in source JSON | Initial BRD derived from provided JSON | Not present in source JSON |