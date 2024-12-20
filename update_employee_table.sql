CREATE OR REPLACE PROCEDURE pdr_scd_demo()
returns string not null
language javascript
as
    $$
      var cmd = `
                 merge into employee e 
                 using employee_raw er
                    on  e.id = er.id
                 when matched and e.name  <> er.name  or
                                  e.email   <> er.email   or
                                  e.phone       <> er.phone       or
                                  e.address      <> er.address      or
                                  e.dob        <> er.dob        or
                                  e.salary       <> er.salary       or
                                  e.department     <> er.department  or
                                  e.joining_date       <> er.joining_date       or
                                  e.is_active       <> er.is_active  
                                  then update
                     set e.name = er.name
                        ,e.email  = er.email   
                        ,e.phone  = er.phone      
                        ,e.address  = er.address     
                        ,e.dob  = er.dob       
                        ,e.salary  = er.salary      
                        ,e.department  = er.department 
                        ,e.joining_date  = er.joining_date   
                        ,e.is_active  = er.is_active
                        ,update_timestamp = current_timestamp()
                 when not matched then insert
                            (e.id, e.name, e.email, e.phone, e.address, e.dob, e.salary, e.department, e.joining_date, e.is_active)
                     values (er.id, er.name, er.email, er.phone, er.address, er.dob, er.salary, er.department, er.joining_date, er.is_active);
      `
      var cmd1 = "truncate table employee_nifi_scd.public.employee_raw;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;
-- call pdr_scd_demo();

create or replace task tsk_scd_raw warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
call pdr_scd_demo();

show tasks;

alter task tsk_scd_raw suspend;

select * from employee_nifi_scd.public.employee;
