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
    * Business rule screens implement more complex tests that do not fit the simpler column or structure screen categories.
* Each quality screen has to decide what happens when an error is thrown. The choices are:
    * Halt the process.
    * Send the offending record(s) to a suspense file for later processing.
    * Tag the data and pass it through to the next step in the pipeline. 

### Identifying column screens:
**Process**
* Create a screen to evaluate record validity and to generate *error event facts*.
* Based on assumptions and observations found in the data profile, determine patterns and logic that columns must follow.
