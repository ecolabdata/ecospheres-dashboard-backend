-- create a database for every env (match production names for easy restore)

CREATE DATABASE dashboard_backend;
GRANT ALL PRIVILEGES ON DATABASE dashboard_backend TO postgres;

CREATE DATABASE dashboard_backend_prod;
GRANT ALL PRIVILEGES ON DATABASE dashboard_backend_prod TO postgres;
