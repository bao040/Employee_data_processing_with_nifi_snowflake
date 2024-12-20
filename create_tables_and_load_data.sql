create database if not exists employee_nifi_scd;
use database employee_nifi_scd;

create or replace table employee_raw(
    id integer,
    name string,
    email string, 
    phone string, 
    address string,
    dob string,
    salary numeric,
    department string, 
    joining_date string, 
    is_active boolean
);

create or replace table employee (
    id integer,
    name string,
    email string, 
    phone string, 
    address string,
    dob string,
    salary numeric,
    department string, 
    joining_date string, 
    is_active boolean,
    update_timestamp timestamp_ntz default current_timestamp()
);

create or replace table employee_logs (
    id integer,
    name string,
    email string, 
    phone string, 
    address string,
    dob string,
    salary numeric,
    department string, 
    joining_date string, 
    is_active boolean,
    start_time timestamp_ntz default current_timestamp(),
    end_time timestamp_ntz default current_timestamp(),
    is_current boolean
);

create or replace stream employee_changes on table employee;

-- Load data from s3 with snowpipe

desc integration EMPLOYEE_STORAGE_INTEGRATION;

CREATE OR REPLACE FILE FORMAT employee_nifi_scd.stages_pipes.csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
--DATE_FORMAT = 'MM/DD/YYYY'
EMPTY_FIELD_AS_NULL = TRUE
;

create or replace stage employee_nifi_scd.stages_pipes.employee_stage
url = 's3://employee-nifi-snowflake/'
storage_integration = EMPLOYEE_STORAGE_INTEGRATION
file_format = employee_nifi_scd.stages_pipes.csv_file_format
;

CREATE OR REPLACE PIPE employee_nifi_scd.stages_pipes.employee_pipe
AUTO_INGEST = TRUE
AS 
COPY INTO employee_nifi_scd.public.employee_raw
FROM @employee_nifi_scd.stages_pipes.employee_stage;

show pipes;

select * from employee_nifi_scd.public.employee_raw;





