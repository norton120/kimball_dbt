# Data Warehouse 2 construction

## Screens
**Documentation will leave inside each screen (by table).**

## Data Profiling process

### Overview:
* Generate the data profile for a table in HTML through pandas_profiling.
* Use Jumper and Python stat_profile_gen.py to build profiles.
* Data profile HTML files to live in DW2 GitHub repository.
* Update DW2/data_profiles and tag JIRA ticket in commit message.

### Steps to create and document a new data profile for a table:
* **Select a table to profile.**
* **Open Jumper from Desktop (to open connection to Postgres).**
* In Terminal:
* `cd DW`
* `git pull`
* `git fetch`
* `git checkout branch_name_TABLE_NAME`
* **Now local DW2 repository (data_profiles_base branch) is up to date with most recent changes on GitHub.**
* This will also provide the most recent version of utilities/stat_profile_gen.py
* `cd data_profiles`
* `python ../utilities/stat_profile_gen.py TABLE_NAME`
* `ls` (to confirm that the new HTML profile for TABLE_NAME has been successfully created)
* `chromium-browser TABLE_NAME.html` (to view the new profile for TABLE_NAME)
* `git status`
* `git add --all`
* `git status`
* `git commit TABLE_NAME.html -m "JIRA_TICKET"`
* `git status`
* `git push origin branch_name_TABLE_NAME`
* `git status` ("nothing to commit, working tree clean")

#### Local Python server for ~/DW2/data_profiles
`python -m SimpleHTTPServer 8000`
