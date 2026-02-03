# Functional Requirements Document — Application

+ When the user provides a PDF or DOCX file name, return a short summary of the custom template.
	+ Ask the user if they want to use the default FRD template or a custom template.
	+ If the user selects "default", generate the FRD using the default template.
	+ If the user selects "custom", ask the user to provide a custom template example.
	+ Generate the FRD based on the user-selected template (default or custom).
	+ Preserve the user's template structure, headings, and order for custom templates.
	+ After generating and presenting the full FRD, ask the user: "
