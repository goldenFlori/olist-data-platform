-- Create catalog, schemas and volume for Olist project

CREATE CATALOG IF NOT EXISTS olist;

CREATE SCHEMA IF NOT EXISTS olist.landing;
CREATE SCHEMA IF NOT EXISTS olist.bronze;
CREATE SCHEMA IF NOT EXISTS olist.silver;
CREATE SCHEMA IF NOT EXISTS olist.gold;
CREATE SCHEMA IF NOT EXISTS olist.control;
CREATE SCHEMA IF NOT EXISTS olist.quarantine;

CREATE VOLUME IF NOT EXISTS olist.landing.files;
