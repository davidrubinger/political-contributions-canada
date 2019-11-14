# political-contributions-canada
Udacity Data Engineering Nanodegree Capstone Project

## Setup

* Create an Airflow variable in the UI with `Key` set to `project_dir` and `Val` set to an absolute path for where your project is, e.g., `/Users/david.rubinger/projects/political-contributions-canada`. This is used to define where data files will be unzipped into.
* Create a connection to Postgres in the UI:
    * `Conn Id` should match the `conn_id` passed to the `transform_contributions_task` task
    * `Conn Type` should be `Postgres`
    * `Host` should be `localhost`
