# Introduction
This is the automation of a simple yet scalable data lake and data warehouse using Python, PostgreSQL, AWS S3, Airflow, Parquet, Kimball model, Docker and pre-commit with hooks such as Black.

Here is why I used each language:
* Python: For creating the pipelines connecting the different layers
* PostgreSQL: For creating and querying the tables in the data warehouse
* AWS S3: As the Data Lake
* Airflow: For scheduling, monitoring data and computing workflows.
* Parquet: To efficiently save the JSON data
* Kimball model: To create a bottom-up approach to data warehouse architecture design.
* Docker: To build containerized applications in order to make the project portable and highly conducive to automation
* pre-commit: To make high quality standardized code

# Configuration


## Docker
The code is build using Docker images such as PostgreSQL and Airflow, using docker-compose.yaml to start the containers using the command:
```
docker compose up -d
```
The docker image of airflow is a custom one in order to include the requirements.txt. You can find the details in the Dockerfile.
You can access the airflow web by entering in your browser:
http://localhost:8080/
username: airflow
password: airflow

The main_dag is the job that encapsulates all tasks and performs the complete process from data lake to dwh.

# Planning

## Layers

The Data Lake:
* Raw layer: Extract the info from the given S3 bucket into our Data Lake.

The DWH architecture is divided in:
* Raw layer: For extracting data from our data lake
* Staging layer: For cleaning and pre-processing raw data
* Semantic layer: For consolidating and integrating clean data with business rules
* Data warehouse layer: For exposing SVOT to the target end users (such as BI team)
* Presentation layer: Includes the DataMarts and reporting codes for creating data stories with the SVOT data
* Governance layer: For setting policies and practices to keep enterprise data accurate, consistent and high quality. I have used 3 roles, data scientist, data analyst and data engineers, each having its own junior and senior staff
* Orchestration layer: for managing the execution of workflows via event-based triggers or time intervals. all the dags but "main_dag" are individual tasks that perform single operations per module in order to test  them individually.

I have included data profiling metrics that display the properties and statistics involved in each stage by logging it so that I can monitor the quality of the data moving between stages.

## Data Lake
### Raw layer
* Download from given S3 the data (JSON).
* Convert it to a more appropriate  format (parquet).
* Upload it to our data lake.
* Highlight sensitive fields.
* Add event logging.
* Run data profiling checks.

## Data Warehouse
### Raw layer
* Download the data from out Data Lake using S3.
* Upload it into the L1 layer raw_db dwh.

### Staging layer
* enable_cross_database from raw_db to staging_db.
* Clean the data and shape it into a format suitable for adding business rules.
* Python which will carry the heavy-lifting transformations for this project. SQL will simply be used for creating tables and inserting data from the raw tables into staging tables.
* Upload into the dev staging_db.
* If everything is ok, upload into prod staging_db.
* The PROD environment will be the ideal environment used for any reporting and analysis, while the DEV will be used for data scientists and analysts to run their tests and experiments.

### Semantic layer
* enable_cross_database from staging_db to semantic_db.
* Add business rules to the L2 staging
* Form the single version of truth. This is done by using the staging tables to create the MDM tables that will be used for dimensional modelling.
* The DEV database follows similar reasons as the previous layer
* The PROD database will contain the enriched data that forms the single version of truth (SVOT).
* The MDM tables in the PROD database will serve as the single version of truth (SVOT).

### Dwh layer
* enable_cross_database from semantic_db to dwh_db.
* Data from the SVOT is available for the target users to perform their intended operations, such as reporting, analysis, investigations and audits, among others.
* Apply dimensional modelling

### Presentation layer
* This stage is for visualizing the data from the previous stage.
* I used plotly to create interactive .html files located in dwh_pipelines/L4_dwh_layer/reporting
* no_updates_per_clients: It talks about the top clients with more events in a period of time. So, we can detect which clients use more the software and gather more insights of their data. The graph represents how many times the client's vehicle has had an update in the period of time.
* lastest_updates_on_top1_client: It talks about a world map, featuring the path made by the vehicle (top 1 with most updates) in the latest hour so we can visualize it and make hypothesis around it.

### Governance layer
I set up the following governance policies:

* **Role-based access control**: For creating custom roles and assigning privileges based on each team's distinct responsibilities.
* **Table ownership**: For allocating table ownership privileges to the appropriate roles.


## Role-based access control

I created custom roles that possess a distinct set of privileges based on how each team will access the data warehouse.

The steps for this stage are as followed:

1. Create custom roles using the `CREATE` command

2. Grant roles access to use the appropriate databases and schemas using the `GRANT` command

3. Grant appropriate privileges to custom roles (options from `SELECT`, `UPDATE`, `INSERT`, `DELETE` etc) using the `GRANT` command

4. Restrict roles from accessing certain databases, schemas and tables using the `REVOKE` command


For simplicity’s sake, I've created the following custom roles in this project...:

* junior\_data\_analyst

* senior\_data\_analyst

* junior\_data\_engineer

* senior\_data\_engineer

* junior\_data\_scientist

* senior\_data\_scientist


and supplied general constraints required for the following roles:

* Data Analyst can only access dwh_db.
* Data Scientist can access dwh_too and semantic and staging db.
* Data Engineer can access the previous 3 plus the raw_db

# Orchestration layer

I used Airflow to manage how each DL and DWH layers will interact and under what conditions too.

# Conclusion

I used AWS S3 as my data lake and PostgreSQL as the DWH. While it serves the purpose of the given challenge as a POC, there is room to improve the solution using other dwh options such as Snowflake. The RDMS system can serve as a high performant data warehouse that meets business reporting needs in many use cases.

We can also improve it by using more robust technologies to process huge quantity of data such as Spark, to improve the time needed to process and the scalability  of the project.

On the other hand, we can make partitions in the database to optimize the query execution time and good user control. Also, we can implement a more modern architecture such as the medallion one (bronze, silver and gold layers).

Finally, we can improve it further by using dbt to transform the data in the dwh, effectively creating your entire transformation process with code.


# Airflow docker doc

Follow the doc to put the config based on your OS
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
