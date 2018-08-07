### Reference document for creating data profiles and screens.

## Data Profiling process
### Overview:
* Generate the data profile for a table in HTML through pandas_profiling.
* Use Jumper and Python stat_profile_gen.py to build profiles.
* Data profile HTML files to live in DW2 GitHub repository.
* Update DW2/data_profiles and tag JIRA ticket in commit message.

### Steps to create and document a new data profile for a table:
* **Select a table to profile.**
* Open Jumper from Desktop (to open connection to Postgres).
* In Terminal:
    * `cd DW2`(move into the DW2 Git repository)
    * `git pull` (to update your remote tracking branches)
    * `git checkout branch_name_TABLE_NAME` (switch to the working branch)
    * Use `git status` to check what branch is currently active and any pending changes.

**Now the local DW2 repository (data_profiles_base branch) is up to date with the most recent changes on GitHub.**
* This will also provide the most recent version of utilities/stat_profile_gen.py
* In Terminal:
    * `cd data_profiles`
    * `python ../utilities/stat_profile_gen.py TABLE_NAME`
    * `ls` (to confirm that the new HTML profile for TABLE_NAME has been successfully created)
    * `chromium-browser TABLE_NAME.html` (to view the new profile for TABLE_NAME)
    * `git status`
    * `git add --all`
    * `git status`
    * `git commit TABLE_NAME.html -m "Create TABLE_NAME data profile."`
    * `git status`
    * `git push origin branch_name_TABLE_NAME`
    * `git status` "nothing to commit, working tree clean"

### Local Python server for ~/DW2/data_profiles
* `python -m SimpleHTTPServer 8000`
* Open up your web browser and enter http://localhost:8000/ to view available profiles. (Use Ctrl + c to quit.)


## Screens
**Documentation will live inside each screen (by table).**
### Overview:
* Create a screen to evaluate record validity and to generate error event facts.
* Based on assumptions and observations found in the data profile, determine patterns and logic that columns must follow.

### Steps for creating a new table screen:
* In Terminal:
    * `cd DW2`
    * `python kdbt_gen.py --help` (for help documentation)
        * "usage: kdbt_gen.py <model_type> <model_name> [--option_name option_value]"
        * `python kdbt_gen.py screen TABLE_NAME`
    * If an option needs to be used, after TABLE_NAME add `--option_name option_value`
* Running the kdbt_gen.py script will generate a SQL file in DW2/screens.

### Evaluate the data profile for TABLE_NAME
* Review the HTML report on TABLE_NAME and note the fields that need to be screened for validity.
* Within the new table screen, comment on the fields that need to be screened and include the relevant screens.
    * If a required screen has already been created, add that screen to the table screen SQL file.
    * If not such screen has been created yet, create a new screen macro.

### Creating a new screen to test for validity within a column.
* Template: `{% set screen_name = {'column':'column_name', 'type':'macro_name'} %}`
* For each screen, list the screen under screen collection: `{% set screen_collection =  [screen_name1, screen_name2, ...]%}`
* If not such screen has been created yet, create a new screen macro.
* All screens must be added to the screen collection in the next section.

### Creating a new screen macro.
* Name the macro and thoroughly describe what it does at the top of the SQL file.
* Add a signature at the top of the macro file to describe what is being done and what arguments to pass through the macro.
* Add new screen macros to the screen_declaration so that a CTE is created for each column screen passed.


## Audits
* Set the variables for each source to be audited.
* Combine the list of the sources to be audited in the next section below.


## Staging Quality
* Staging Quality tables are generated through kdbt
* In Terminal:
    * Start from DW2
    * `python kdbt_gen.py staging_quality TABLE_NAME`


## Run the Model
* Materialize the model with dbt
* In Terminal:
    * Start from DW2
    * `dbt run`
