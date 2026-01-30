# Functional Requirements Document — Application

### Custom FRD Template Output

When the user provides a PDF or DOCX file name, return a short summary of the custom template and ask if they want to use the default or custom template.

* Your FRD will contain these sections: 1. Requirement ID, 2. Title, 3. Description, 4. Preconditions, 5. Main Flow / Functional Steps
* Do you want me to use the default FRD template or a custom template you provide?

### After User Chooses Template

#### Default Template

If the user chooses the default template, generate the FRD using the default template.

#### Custom Template

If the user chooses a custom template, ask them to provide the custom template example.

* Please provide your custom template

### FRD Generation

After the user provides the input PDF or DOCX file and chooses a template, generate the FRD.

#### Default FRD Template

Here is the generated FRD using the default template:

1. **Requirement ID**: REQ001
2. **Title**: Load Customer and Order Data into Delta Tables and Generate Aggregate Spend
3. **Description**: This requirement involves loading customer and order data from CSV files into Delta tables, joining the data, and generating an aggregate spend report.
4. **Preconditions**:
	* Customer and order CSV files are available at the specified locations.
	* The Delta tables do not have any existing data that would conflict with the new data.
5. **Main Flow / Functional Steps**:
	* Read source CSV data from volume and load to Delta tables: customer and order.
	* Remove “Null”/Null and Duplicate records from both tables.
	* Create “ordersummary” table if not exists in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
	* Join customer and order data using “CustId” field and load the data in SCD type 2 table under catalog=gen_ai_poc_databrickscoe, schema= sdlc_wizard, table=ordersummary.
	* Include a logic to update the SCD type 2 table ordersummary whenever there is a change in the customer table.
	* Create a table if not exists new table “customeraggregatespend” with columns “Name”, “TotalAmount” and “Date” in catalog=“gen_ai_poc_databrickscoe” and schema=“sdlc_wizard”.
	* Aggregate the “TotalAmount” column from “ordersummary” table and group by “Name” and “Date” columns.
	* Load the aggregated data from the “ordersummary” table having columns “Name”, “TotalAmount” and “Date” and load in “customeraggregatespend”.

#### Custom FRD Template

If the user provides a custom template, generate the FRD according to the provided template.

### After FRD Generation

After generating and presenting the full FRD to the user (default or custom), ask:

* Would you like me to push this FRD to GitHub as a Markdown file from memory?