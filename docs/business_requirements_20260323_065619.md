Business Requirements Document (BRD)

Section 1. Executive Summary
This document translates the provided JSON into a complete set of business requirements for ingesting customer and order CSV data into Delta tables, performing data cleansing, computing a TotalAmount, joining data to create an ordersummary table with SCD Type 2 handling for customer changes, and creating an aggregate table customeraggregatespend with daily spend totals. Implementation is directed via Delta Live Tables. All names, paths, schemas, and rules are preserved exactly as in the source JSON.

Section 2. Business Objectives
- Load source CSVs into Delta tables for customer and order data.
- Cleanse data by removing "Null"/Null literals and duplicates.
- Enrich orders with a computed TotalAmount = PricePerUnit × Qty.
- Produce an ordersummary table by joining customer and order on CustId.
- Apply SCD Type 2 change handling for customer data, maintaining full history.
- Create a customeraggregatespend table aggregating spend per Name per Date.
- Execute the end-to-end pipeline using Delta Live Tables.

Section 3. In Scope
- Source ingestion from specified file system locations.
- Creation and management of Delta tables: customer, order, ordersummary, customeraggregatespend.
- Data cleansing actions as stated.
- Computation of TotalAmount in order.
- Join logic and SCD Type 2 handling for customer changes in ordersummary.
- Aggregate processing from ordersummary to customeraggregatespend.
- Implementation guidance tied to Delta Live Tables.

Out of Scope
- Any data sources, systems, or fields not present in source JSON.
- Security, access controls, SLAs, monitoring, and alerting.
- Performance tuning and scaling parameters.
- Scheduling, orchestration, or DevOps details beyond “Implement steps 1–10 using Delta Live Tables.”
- Data quality metrics beyond the cleansing actions listed.
- User interfaces, reports, or analytics consumption layers.

Section 4. Stakeholders
Information not present in source JSON.

Section 5. Current State
Information not present in source JSON.

Section 6. Future State Vision
- Structured, cleansed, and historical customer-order dataset in Delta tables.
- Consistent SCD Type 2 history for customer attributes within ordersummary.
- Daily customer spend aggregates in customeraggregatespend.
- Pipeline implemented via Delta Live Tables following defined steps.

Section 7. Functional Requirements
FR-1 Source Ingestion
- Load source CSVs into Delta tables.
- Customer source: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata → target Delta table customer.
- Order source: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata → target Delta table order.

FR-2 Schemas
- customer columns: CustId, Name, EmailId, Region.
- order columns: OrderId, ItemName, PricePerUnit, Qty, Date, CustId.

FR-3 Computed Column
- On table order, add computed column:
  - name: TotalAmount
  - expression: PricePerUnit × Qty

FR-4 Data Cleansing
- On tables customer and order:
  - Remove "Null"/Null literals.
  - Remove duplicate records.

FR-5 Create Target Table ordersummary
- Action: Create table if not exists.
- Location: catalog=gen_ai_poc_databrickscoe, schema=sdlc_wizard.
- Table name: ordersummary.

FR-6 Join and Load with SCD Type 2
- Join:
  - left_table: customer
  - right_table: order
  - on: CustId
- Target:
  - catalog: gen_ai_poc_databrickscoe
  - schema: sdlc_wizard
  - table: ordersummary
- loading_type: SCD Type 2

FR-7 ordersummary Schema
- Fields: CustId, Name, EmailId, Region, OrderId, ItemName, PricePerUnit, Qty, Date.

FR-8 SCD Type 2 Change Handling Rules (customer dimension)
- Set previous records to Inactive and new records to Active.
- Update StartDate and EndDate accordingly.
- Maintain full history.

FR-9 Create Aggregate Table customeraggregatespend
- Action: Create table if not exists.
- Table: customeraggregatespend
- Columns: Name, TotalAmount, Date
- Location: catalog=gen_ai_poc_databrickscoe, schema=sdlc_wizard.

FR-10 Aggregate Loading
- Source: ordersummary
- Aggregation:
  - group_by: Name, Date
  - metrics: TotalAmount with aggregation=sum
- Target: customeraggregatespend
- columns_loaded: Name, TotalAmount, Date

FR-11 Implementation Modality
- Implement steps 1–10 using Delta Live Tables.

Section 8. Non-Functional Requirements
- Information not present in source JSON.

Section 9. Business Rules
BR-1 Computation
- TotalAmount = PricePerUnit × Qty.

BR-2 Cleansing
- Remove "Null"/Null literals from customer and order.
- Remove duplicate records from customer and order.

BR-3 Join
- Join customer to order on CustId.

BR-4 SCD Type 2 Handling for customer
- Set previous records to Inactive and new records to Active.
- Update StartDate and EndDate accordingly.
- Maintain full history.

BR-5 Aggregation
- For customeraggregatespend: group by Name and Date, compute sum(TotalAmount).

Section 10. Data Requirements
- Source paths as defined for customer and order CSVs.
- Delta tables with specified schemas for customer, order, ordersummary, and customeraggregatespend.
- ordersummary schema fields exactly as listed.
- Aggregated fields and groupings as specified.

Section 11. Assumptions and Constraints
- No assumptions beyond explicit JSON content.
- Delta Live Tables is to be used as the implementation approach per instruction.
- Any additional attributes such as StartDate, EndDate, Active flags are referenced in rules but not explicitly listed in schemas; their explicit field definitions are not present in source JSON.

Section 12. Dependencies and Lineage
- Ingestion dependencies: customer and order tables depend on their respective source CSV paths.
- ordersummary depends on both customer and order with a join on CustId and SCD Type 2 handling.
- customeraggregatespend depends on ordersummary and the defined aggregation.
- Load order: customer/order → ordersummary → customeraggregatespend.
- Name mismatches between lineage pairs: None detected based on provided names.
- Any additional DAG order details not present are marked as not present.

Section 13. Risks and Issues
- Potential ambiguity in SCD Type 2 attribute fields (StartDate, EndDate, Active) definitions: Information not present in source JSON.
- Data quality beyond removing "Null"/Null literals and duplicates: Information not present in source JSON.
- Error handling, retries, and monitoring: Information not present in source JSON.

Section 14. Environment and Deployment Considerations
- Environments: Information not present in source JSON.
- Platform cue: Delta Live Tables is specified for implementation. Additional platform/environment details: Information not present in source JSON.

Section 15. Glossary
- SCD Type 2: Maintain historical versions of records by closing previous records and activating new versions. (Terminology usage conforms to source JSON; specific field implementations are not defined in the JSON.)
- Delta tables: Not further defined in source JSON.
- TotalAmount: Computed as PricePerUnit × Qty.

Appendix A. Repository and Instruction Metadata
| Key | Value |
| --- | --- |
| Implementation Instruction | Implement steps 1–10 using Delta Live Tables. |
| Additional Repository Metadata | Not present in source JSON. |

Appendix B. Source Definitions
| Source Name | Path | Format | Notes |
| --- | --- | --- | --- |
| Customer source | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata | Not present in source JSON | Loaded into Delta table customer |
| Order source | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata | Not present in source JSON | Loaded into Delta table order |

Appendix C. Target Definitions
| Catalog | Schema | Table | Description |
| --- | --- | --- | --- |
| Not present in source JSON | Not present in source JSON | customer | Target Delta table from Customer source |
| Not present in source JSON | Not present in source JSON | order | Target Delta table from Order source |
| gen_ai_poc_databrickscoe | sdlc_wizard | ordersummary | Joined and SCD Type 2 managed summary |
| gen_ai_poc_databrickscoe | sdlc_wizard | customeraggregatespend | Aggregate spend per Name and Date |

Appendix D. Source-to-Target Inventory
| Source | Source Object | Target Catalog | Target Schema | Target Table | Notes |
| --- | --- | --- | --- | --- | --- |
| Customer source | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata | Not present in source JSON | Not present in source JSON | customer | Load as Delta |
| Order source | /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata | Not present in source JSON | Not present in source JSON | order | Load as Delta |
| ordersummary | Derived from customer and order | gen_ai_poc_databrickscoe | sdlc_wizard | ordersummary | Join on CustId with SCD Type 2 |
| ordersummary | ordersummary | gen_ai_poc_databrickscoe | sdlc_wizard | customeraggregatespend | Aggregation |

Appendix E. Table Schemas
| Table | Column | Data Type | Nullable | Notes |
| --- | --- | --- | --- | --- |
| customer | CustId | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| customer | Name | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| customer | EmailId | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| customer | Region | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | OrderId | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | ItemName | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | PricePerUnit | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | Qty | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | Date | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| order | CustId | Not present in source JSON | Not present in source JSON | Not present in source JSON |
| ordersummary | CustId | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | Name | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | EmailId | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | Region | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | OrderId | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | ItemName | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | PricePerUnit | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | Qty | Not present in source JSON | Not present in source JSON | Derived via join |
| ordersummary | Date | Not present in source JSON | Not present in source JSON | Derived via join |
| customeraggregatespend | Name | Not present in source JSON | Not present in source JSON | Group-by column |
| customeraggregatespend | TotalAmount | Not present in source JSON | Not present in source JSON | Aggregated sum |
| customeraggregatespend | Date | Not present in source JSON | Not present in source JSON | Group-by column |

Appendix F. Transformations and Expressions
| Step | Table | Column | Expression | Notes |
| --- | --- | --- | --- | --- |
| Add computed column | order | TotalAmount | PricePerUnit × Qty | Expression provided in source JSON |

Appendix G. Data Cleansing Rules
| Table | Rule | Description |
| --- | --- | --- |
| customer | Remove "Null"/Null literals | As stated |
| customer | Remove duplicate records | As stated |
| order | Remove "Null"/Null literals | As stated |
| order | Remove duplicate records | As stated |

Appendix H. Joins and Relationships
| Left Table | Right Table | Join Keys | Join Type | Notes |
| --- | --- | --- | --- | --- |
| customer | order | CustId | Not present in source JSON | Used to populate ordersummary |

Appendix I. SCD Type 2 Configuration
| Dimension | Target Table | Loading Type | Active Flag Field | Start Date Field | End Date Field | Additional Rules |
| --- | --- | --- | --- | --- | --- | --- |
| customer | ordersummary | SCD Type 2 | Not present in source JSON | Not present in source JSON | Not present in source JSON | Set previous to Inactive, new to Active; update StartDate/EndDate; maintain full history |

Appendix J. Aggregations
| Source Table | Target Table | Group By | Metric Name | Aggregation | Output Columns |
| --- | --- | --- | --- | --- | --- |
| ordersummary | customeraggregatespend | Name, Date | TotalAmount | sum | Name, TotalAmount, Date |

Appendix K. Load Order and Flow
| Order | From | To | Method | Notes |
| --- | --- | --- | --- | --- |
| 1 | Customer source path | customer | Load CSV to Delta | Create if not exists not stated for these tables |
| 1 | Order source path | order | Load CSV to Delta | Create if not exists not stated for these tables |
| 2 | order | order (same) | Add computed column | TotalAmount |
| 3 | customer/order | customer/order | Data cleansing | Remove "Null"/Null and duplicates |
| 4 | N/A | ordersummary | Create table if not exists | Catalog/schema provided |
| 5 | customer + order | ordersummary | Join on CustId | With SCD Type 2 handling |
| 6 | ordersummary | customeraggregatespend | Aggregation | Create table if not exists |

Appendix L. Parameters and Variables
| Name | Default Value | Scope | Description |
| --- | --- | --- | --- |
| Not present in source JSON | Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix M. Configuration Flags
| Component | Flag/Setting | Value |
| --- | --- | --- |
| Create table | action | Create table if not exists |
| ordersummary location | catalog | gen_ai_poc_databrickscoe |
| ordersummary location | schema | sdlc_wizard |
| customeraggregatespend location | catalog | gen_ai_poc_databrickscoe |
| customeraggregatespend location | schema | sdlc_wizard |
| Join and load | loading_type | SCD Type 2 |

Appendix N. Lineage Map
| Upstream | Downstream | Transformation | Notes |
| --- | --- | --- | --- |
| Customer source path | customer | Load | Path: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/customerdata |
| Order source path | order | Load | Path: /Volumes/gen_ai_poc_databrickscoe/sdlc_wizard/orderdata |
| order | order | Computed column | TotalAmount = PricePerUnit × Qty |
| customer | ordersummary | Join | On CustId |
| order | ordersummary | Join | On CustId |
| ordersummary | customeraggregatespend | Aggregation | sum(TotalAmount) by Name, Date |

Appendix O. Connectivity and Flow Depth
| Layer | Object | Upstream Count | Downstream Count |
| --- | --- | --- | --- |
| Source | Customer source path | 0 | 1 |
| Source | Order source path | 0 | 1 |
| Bronze/Silver equivalent | customer | 1 | 1 |
| Bronze/Silver equivalent | order | 1 | 1 |
| Silver/Gold equivalent | ordersummary | 2 | 1 |
| Gold | customeraggregatespend | 1 | 0 |

Appendix P. Quality Checks
| Check | Table | Rule | Expression |
| --- | --- | --- | --- |
| Remove Null literals | customer | Remove "Null"/Null | Expression not provided in source JSON. |
| Remove duplicates | customer | Deduplicate | Expression not provided in source JSON. |
| Remove Null literals | order | Remove "Null"/Null | Expression not provided in source JSON. |
| Remove duplicates | order | Deduplicate | Expression not provided in source JSON. |

Appendix Q. Scheduling, Monitoring, and Alerts
| Topic | Detail |
| --- | --- |
| Scheduling | Not present in source JSON |
| Monitoring | Not present in source JSON |
| Alerts | Not present in source JSON |

Appendix R. Security and Compliance
| Topic | Detail |
| --- | --- |
| Data Classification | Not present in source JSON |
| Access Control | Not present in source JSON |
| Encryption | Not present in source JSON |

Appendix S. Error Handling and Recovery
| Topic | Detail |
| --- | --- |
| Error Handling | Not present in source JSON |
| Retry/Recovery | Not present in source JSON |

Appendix T. Performance and Scaling
| Topic | Detail |
| --- | --- |
| Performance Targets | Not present in source JSON |
| Scaling Strategy | Not present in source JSON |

Appendix U. Testing and Acceptance
| Test Type | Scope | Criteria |
| --- | --- | --- |
| Unit | Computed column and cleansing | Information not present in source JSON |
| Integration | Joins and SCD Type 2 flow | Information not present in source JSON |
| UAT | End-to-end pipeline | Information not present in source JSON |

Appendix V. Stakeholder Register
| Name | Role | Contact |
| --- | --- | --- |
| Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix W. Environment Matrix
| Environment | Catalog | Schema | Notes |
| --- | --- | --- | --- |
| Not present in source JSON | Not present in source JSON | Not present in source JSON | Not present in source JSON |

Appendix X. Open Questions
| ID | Question |
| --- | --- |
| Q1 | What are the explicit data types and nullability for all columns? |
| Q2 | What are the exact field names for SCD Type 2 tracking (e.g., StartDate, EndDate, Active)? |
| Q3 | Are there partitioning, Z-order, or clustering preferences for Delta tables? |
| Q4 | What are the expected data volumes and performance SLAs? |
| Q5 | What is the desired scheduling cadence and monitoring approach in Delta Live Tables? |
| Q6 | What are required access controls and governance policies? |

Appendix Y. Change Log
| Version | Date | Author | Summary |
| --- | --- | --- | --- |
| 1.0 | Not present in source JSON | Not present in source JSON | Initial BRD based on provided JSON |

Appendix Z. Compliance with Repository Rules
| Rule Area | Compliance Note |
| --- | --- |
| No assumptions beyond source JSON | Complied; all unspecified fields marked “Not present in source JSON.” |
| Preserve names and case | Complied. |
| Systems from keys | Delta Live Tables identified from “Implement steps 1–10 using Delta Live Tables.” |
| Environments from cues | None present; marked as not present. |
| Requirements from expressions, filters, conditions | Computed column, cleansing, join, SCD rules, aggregation captured. |
| Dependencies from lineage/DAG | Captured in Appendices K and N. |
| Aggregation expression declared | sum(TotalAmount) declared; any missing expressions explicitly noted. |
| Parameters/variables enumerated | None provided; marked not present. |
| Name mismatches recorded | None detected in provided lineage. |