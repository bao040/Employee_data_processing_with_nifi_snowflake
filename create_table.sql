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
