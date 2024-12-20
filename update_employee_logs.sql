show streams;
select * from employee_changes;

create or replace view v_employee_change_data as

select id, name, email, phone, address, dob, salary, department, joining_date, is_active,
    start_time, end_time, is_current, 'I' as dml_type
from (
select id, name, email, phone, address, dob, salary, department, joining_date, is_active,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then TRUE else FALSE end as is_current
      from (select id, name, email, phone, address, dob, salary, department, joining_date, is_active,                           update_timestamp 
            from employee_nifi_scd.public.employee_changes
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'FALSE')
  )

  
union


select id, name, email, phone, address, dob, salary, department, joining_date, is_active, start_time, end_time, is_current, dml_type
from (select id, name, email, phone, address, dob, salary, department, joining_date, is_active,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then TRUE else FALSE end as is_current, 
             dml_type
      from (-- Identify data to insert into customer_history table
            select id, name, email, phone, address, dob, salary, department, joining_date, is_active,               update_timestamp, 'I' as dml_type
            from employee_nifi_scd.public.employee_changes
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'TRUE'
            union
            -- Identify data in customer_HISTORY table that needs to be updated
            select id, null, null, null, null, null,null,null,null,null, start_time, 'U' as dml_type
            from employee_nifi_scd.public.employee_logs
            where id in (select distinct id 
                                  from employee_nifi_scd.public.employee_changes
                                  where metadata$action = 'DELETE'
                                  and metadata$isupdate = 'TRUE')
     and is_current = TRUE))

     
union

select ec.id, null, null, null, null, null,null,null, null, null, el.start_time, current_timestamp()::timestamp_ntz, null, 'D'
from employee_nifi_scd.public.employee_logs el
inner join employee_nifi_scd.public.employee_changes ec
   on el.id = ec.id
where ec.metadata$action = 'DELETE'
and   ec.metadata$isupdate = 'FALSE'
and   el.is_current = TRUE;

-- id, name, email, phone, address, dob, salary, department, joining_date, is_active, update_timestamp    UPDATE_TIMESTAMP

select * from v_employee_change_data;


-- CREATE SCD2 TASK 
create or replace task tsk_scd2_hist warehouse= COMPUTE_WH schedule='1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
merge into employee_nifi_scd.public.employee_logs el -- Target table to merge changes from NATION into
using v_employee_change_data ecd -- v_customer_change_data is a view that holds the logic that determines what to insert/update into the customer_history table.
   on el.id = ecd.id -- CUSTOMER_ID and start_time determine whether there is a unique record in the customer_history table
   and el.start_time = ecd.start_time
when matched and ecd.dml_type = 'U' then update -- Indicates the record has been updated and is no longer current and the end_time needs to be stamped
    set el.end_time = ecd.end_time,
        el.is_current = FALSE
when matched and ecd.dml_type = 'D' then update -- Deletes are essentially logical deletes. The record is stamped and no newer version is inserted
   set el.end_time = ecd.end_time,
       el.is_current = FALSE
when not matched and ecd.dml_type = 'I' then insert -- Inserting a new CUSTOMER_ID and updating an existing one both result in an insert
          (id, name, email, phone, address, dob, salary, department, joining_date, is_active, start_time, end_time, is_current)
    values (ecd.id, ecd.name, ecd.email, ecd.phone, ecd.address, ecd.dob, ecd.salary, ecd.department, ecd.joining_date, ecd.is_active, ecd.start_time, ecd.end_time, ecd.is_current);
    
show tasks;
alter task tsk_scd2_hist suspend; --resume
