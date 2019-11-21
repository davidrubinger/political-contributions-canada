# Political Contributions Canada

## Setup

* Create an Airflow variable for the project directory, which is used to specify where data files will be unzipped into, by doing either of the following:
    * In the UI, create a variable with `Key` set to `project_dir` and `Val` set to an absolute path for where your project is, e.g., `</absolute/path/to/project/directory>`
    * In the command line interface, enter, for example, `airflow variables --set project_dir </absolute/path/to/project/directory>`
* Create a connection to a local Postgres database by doing either of the following:
    * In the UI, set `Conn Id` to anything, which must match the `conn_id` passed to the `transform_contributions_task` task.; `Conn Type` to `Postgres`; `Host` to `localhost`; `Login` to a login name for Postgres; `Schema` to the database name; and `Port` to the database port number
    * In the command line interface, enter, for example, `airflow connections --add --conn_id <your_connection_id> --conn_type Postgres --conn_host localhost --conn_login <your_login> --conn_schema <database_name> --conn_port <port_number>`

## Purpose

The purpose of this project is to provide all sorts of analysts with a public
data warehouse they can easily query and analyze the large and regularly updating
contributions to Canadian federal political entities.

Some questions that can be asked:

* How have party contributions changed over time?
* Which electoral districts saw the most contributions last year?
* How do provinces compare in contributions made per 1,000 people in 2016?
* What are the trends in contributions per 1,000 people over the past 5 years?

## Source Data

This project draws uses Canadian political contributions data from
[Elections Canada](https://www.elections.ca/home.aspx) and Canadian population
data from [Statistics Canada](https://www.statcan.gc.ca/eng/start). Details on
the two data sets is found below

**Political Contributions Data (Elections Canada)**

* Name: _Contributions to all political entities from January 2004 to present, as reviewed_
* Location: [Elections Canada](https://www.elections.ca/content.aspx?section=fin&dir=oda&document=index&lang=e)
* Update frequency: Weekly
* Number of rows (as of Nov 20, 2019): 3,102,664
* Description: Contributions to Canadian political entities--such as candidates and political parties--including several features--such as contribution received date, contributor's province, recipient's electoral district and political party, and amount contributed

**Population Data (Statistics Canada)**

* Name: _Population estimates, quarterly_
* Location: [Statistics Canada](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1710000901)
* Update frequency: Quarterly
* Number of rows (as of Nov 20, 2019): 3,685
* Description: Canadian population estimates broken down by quarter and province, going back to 1946

## Data Model

This project generates two Postgres tables:

* `contributions`: aggregated annual contributions to Canadian political entities
    * `year`: year in which contribution was received. Aggregation was done at annual level rather than more granular because some contributions aren't recorded until year-end financial reporting.
    * `contributor_province_code`: internationally approved alpha code for province/territory of contributor
    * `electoral_district`: electoral district of recipient
    * `recipient_party`: political party of recipient
    * `monetary_amount`: monetary value of amount contributed
* `population`: annual population estimates of by province/territory
    * `year`: year of population estimate
    * `province_code`: internationally approved alpha code for province/territory
    * `population`: latest estimate of population for given year and province/territory

## Structure

### ETL

* Source data is downloaded and unzipped locally
* Unzipped CSVs are read into Spark DataFrames and transformed and outputted to CSVs locally
* Transformed CSVs are copied to a local instance of PostgreSQL
* Data quality checks are run, checking that the tables have rows and checking the number of observations with a year in the future
* All operations are regularly run and monitored with Airflow tasks

### Tools

* **Python** is used as the programming language because its ease-of-use and flexibility
* **PostgreSQL** is used for the data warehouse as it's a well-suppored relational database management system
* **Spark** (specfically *PySpark*) is used to wrange the data because of it's ability to handle big data sets
* **Airflow** is used to run the ETL because of its powerful scheduling and monitoring featuresbasis

## Potential Scenarios

Eventually this project may have to address the following scenarios as it grows and evolves in use:

* **The data was increased by 100x.** This project would benefit from being stored and processed on cloud servers, such as Amazon S3 and Redshift. Moreover, the data transformed in Spark should not be outputted to CSV before copying to Postgres; rather it should be copied directly from Spark to Postgres.
* **The pipelines would be run on a daily basis by 7am every day.** The parameters in the DAG would have to changed to run at a higher frequency (using the `schedule_interval` parameter), which also may entail having to increasing the maximum number of concurrent DAG runs if each takes more than a day to run.
* **The database needed to be accessed by 100+ people.** Again, this project would benefit from being run in the cloud, such as with AWS, so that users would all be working with the same Postgres database.

## Example Queries

**How have party contributions changed over time?**

```
select year, recipient_party, round(sum(monetary_amount))
from contributions
group by year, recipient_party
order by recipient_party, year;
```

**Which electoral districts saw the most contributions last year?**

```
select electoral_district, round(sum(monetary_amount))
from contributions
where year = date_part('year', current_date) - 1
group by electoral_district
order by sum(monetary_amount) desc
limit 10;
```

**How do provinces compare in contributions made per 1,000 people in 2016?**

```
select
    con.contributor_province_code,
    round(sum(con.monetary_amount)) as monetary_amount,
    sum(pop.population) as population,
    round(sum(con.monetary_amount) / sum(pop.population) * 1000, 1) as monetary_amount_per_1000
from contributions con
join population pop on
    con.year = pop.year and con.contributor_province_code = pop.province_code
where con.year = 2016
group by con.contributor_province_code
order by monetary_amount_per_1000 desc;
```

**What are the trends in overall contributions per 1,000 people over the past 5 years?**

```
select
    con.year,
    round(sum(con.monetary_amount) / sum(pop.population) * 1000, 1) as monetary_amount_per_1000
from contributions con
join population pop on
    con.year = pop.year and con.contributor_province_code = pop.province_code
where con.year >= date_part('year', current_date) - 4
group by con.year
order by con.year;
```