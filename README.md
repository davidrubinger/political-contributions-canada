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

## Data

This project draws uses Canadian political contributions data from
[Elections Canada](https://www.elections.ca/home.aspx) and Canadian population
data from [Statistics Canada](https://www.statcan.gc.ca/eng/start). Details on
the two data sets is found below

*Political Contributions Data (Elections Canada)*

* Name: _Contributions to all political entities from January 2004 to present, as reviewed_
* Location: [Elections Canada](https://www.elections.ca/content.aspx?section=fin&dir=oda&document=index&lang=e)
* Update frequency: Weekly
* Number of rows (as of Nov 20, 2019): 3,102,664
* Description: Contributions to Canadian political entities--such as candidates and political parties--including several features--such as contribution received date, contributor's province, recipient's electoral district and political party, and amount contributed

*Population Data (Statistics Canada)*

* Name: _Population estimates, quarterly_
* Location: [Statistics Canada](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1710000901)
* Update frequency: Quarterly
* Number of rows (as of Nov 20, 2019): 3,685
* Description: Canadian population estimates broken down by quarter and province, going back to 1946

## Structure

### ETL

* Source data is downloaded and unzipped locally
* Unzipped CSVs are read into Spark DataFrames and transformed and outputted to CSVs locally
* Transformed CSVs are copied to a local instance of PostgreSQL
* All operations are regularly run and monitored with Airflow tasks

### Tools

* *Python* is used as the programming language because its ease-of-use and flexibility
* *PostgreSQL* is used for the data warehouse as it's a well-suppored relational database management system
* *Spark* (specfically *PySpark*) is used to wrange the data because of it's ability to handle big data sets
* *Airflow* is used to run the ETL because of its powerful scheduling and monitoring featuresbasis

## Potential Scenarios

Eventually this project may have to address the following scenarios as it grows in use:

* *The data was increased by 100x.* 
* *The pipelines would be run on a daily basis by 7 am every day.* 
* *The database needed to be accessed by 100+ people.*

## Example Queries

*How have party contributions changed over time?*

```
select year, recipient_party, round(sum(monetary_amount))
from contributions
group by year, recipient_party
order by recipient_party, year;
```

*Which electoral districts saw the most contributions last year?*

```
select electoral_district, round(sum(monetary_amount))
from contributions
where year = date_part('year', current_date) - 1
group by electoral_district
order by sum(monetary_amount) desc
limit 10;
```

*How do provinces compare in contributions made per 1,000 people in 2016?*

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

*What are the trends in overall contributions per 1,000 people over the past 5 years?*

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