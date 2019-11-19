# ✈️ Dreamliner - Effortless ETL into Redshift and Postgres #

## What is Dreamliner? ##
Dreamliner is a set of Airflow plugins that allow for simple ETL into Postgres/Redshift.

What differentiates Dreamliner is the ability to dynamically create tables in Postgres/Redshift. Instead of defining the table schema beforehand and creating the tables individually, the Dreamliner plugin generates a table schema for the target database based on the SQL query from the source database.

Currently, only ETL between SQL Server and Postgres/Redshift is supported. However, there are plans to support other databases in the future, such as Oracle and MySQL.

## The crew in the cockpit ##
Dreamliner was designed by Andrew Alkhouri (@andyalky). You can connect with him on LinkedIn at www.linkedin.com/in/andyalky.