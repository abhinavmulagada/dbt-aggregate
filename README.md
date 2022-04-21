# dbt for Data Services
dbt repo from Data Services at cLabs

# About
dbt is a tool to create views and table ontop your exisiting data warehouse solution. This project is configured to work with BigQuery.

The views built here are read from DataStudio to create the charts found on our explorer page: https://explorer.celo.org/stats

# Helpful Commands:

Run all dbt models in a repo
>dbt run

Run a specific dbt model:
>dbt run -m sql_filename_here.sql

Re-run a specific model:
>dbt run -m sql_filename_here.sql --full-refresh



# Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
