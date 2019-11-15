# Political Contributions Canada
Udacity Data Engineering Nanodegree Capstone Project

## Setup

* Create an Airflow variable for the project directory, which is used to specify where data files will be unzipped into, by doing either of the following:
    * In the UI, create a variable with `Key` set to `project_dir` and `Val` set to an absolute path for where your project is, e.g., `</absolute/path/to/project/directory>`
    * In the command line interface, enter, for example, `airflow variables --set project_dir </absolute/path/to/project/directory>`
* Create a connection to a local Postgres database by doing either of the following:
    * In the UI, set `Conn Id` to anything, which must match the `conn_id` passed to the `transform_contributions_task` task.; `Conn Type` to `Postgres`; `Host` to `localhost`; `Login` to a login name for Postgres; `Schema` to the database name; and `Port` to the database port number
    * In the command line interface, enter, for example, `airflow connections --add --conn_id <your_connection_id> --conn_type Postgres --conn_host localhost --conn_login <your_login> --conn_schema <database_name> --conn_port <port_number>`
