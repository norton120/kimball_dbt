## Analyzing tables and identifying potential column screens

### What are Screens and why do we need them?
**Overview:**
* "The heart of the ETL architecture is a set of quality screens that act as diagnostic filters in the data flow pipelines."
    * Each quality screen is a test.
        * If the test against the data is successful, nothing happens and the screen has no side effects.
        * If the test fails, then it must drop an error event row into the error event schema and choose to either *halt* the process, send the offending data into *suspension*, or *tag* the data.
    * Column screens test the data within a single column.
        * Column screens test whether a column contains unexpected null values, if a value falls outside of a prescribed range, or if a value fails to adhere to a required format.
        * *Column screens will be the primary focus of data exploration by table.*
    * Structure screens test the relationship of data across columns.
    * Business rule screens implement more complex tests that do not fit the column screen or structure screen categories.
* Each quality screen has to decide what happens when an error is thrown.
    * The choices are:
        * Halt the process.
        * Send the offending record(s) to a suspense file for later processing.
        * Tag the data and pass it through to the next step in the pipeline.
    * Tagging is often the best choice because bad fact data and dimension data can be tagged using an audit dimension, or in the case of missing data can be tagged with unique error values in the attribute itself.

### Identifying column screens:
**Main Idea**
* Create a screen to evaluate record validity and to generate *error event facts*.
* Based on assumptions and observations found in the data profile, determine patterns and logic that columns must follow.

**Process:**
* Select a table to evaluate. Consider the grain of the data and the business context. What are the assumptions surrounding this table?
* Generate the data profile for the table, using /DW2/utilities/stat_profile_gen.py. This will produce a report in HTML through pandas_profiling. Open the HTML file to view the data profile in browser.
* Evaluate each column in the table and consider the sample statistics provided in the HTML report.
    * Are there suspect values? Are there strange values with high record counts? Are there unexpected outliers?
    * If everything appears to be normal, what defines "normal?" Are there logical tests that can confirm that a column is behaving in accordance with accepted assumptions?
* Develop logical tests for each column that a record must pass in order to be considered "valid."
* Within the screen SQL file for the table, provide line comments for each column.
    * Use eight (8) "-" marks to comment out a line and provide the columns name and data type, preferably from Postgres.
    * Next, include all known context for the column and identified logical tests.
        * For each logical test:
            * Use four (4) "-" marks and write out the requirements for the logical text in plain English.
            * If multiple lines are needed to clearly explain the required logic, use six (6) "-" marks to begin each subsequent line of comments to distinguish that these comments are still part of the same logical test.
        * Once all logical tests for a column are provided, return twice to skip a line and begin evaluating the next column.
* The Data Warehouse team thanks you for your participation. **Thank you.**
