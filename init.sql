-- create a database for every env

CREATE DATABASE demo;
GRANT ALL PRIVILEGES ON DATABASE demo TO postgres;

CREATE DATABASE prod;
GRANT ALL PRIVILEGES ON DATABASE prod TO postgres;
