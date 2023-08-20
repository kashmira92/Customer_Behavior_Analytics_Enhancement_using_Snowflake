alter user kashmira275 set default_role = 'SYSADMIN';
alter user kashmira275 set default_warehouse = 'COMPUTE_WH';
alter user kashmira275 set default_namespace = 'UTIL_DB.PUBLIC';
select current_account();
list @uni_kishore/kickoff;

--using s3 bucket location
select $1 from 
@uni_kishore/kickoff
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/kickoff
file_format =(format_name=ff_Json_logs);

select 
raw_log:agent::text as agent,
raw_log:user_event::text as user_event,
raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
raw_log:user_login::text as user_login
,*
from   game_logs;

SELECT current_timestamp()

select $1 from 
@uni_kishore/updated_feed
(file_format => ff_json_logs);

copy into ags_game_audience.raw.game_logs
from @uni_kishore/updated_feed
file_format =(format_name=ff_Json_logs);

select 
raw_log:agent::text as agent,
raw_log:user_event::text as user_event,
raw_log:datetime_iso8601::TIMESTAMP_NTZ as datetime_iso8601,
raw_log:user_login::text as user_login,
raw_log:ip_address::text as ip_address
,*
from   game_logs;

--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

select parse_ip('100.41.16.160','inet');

--Look up in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
           , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;


--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone ,
convert_timezone('UTC', timezone, logs.datetime_iso8601 ) as game_event_ltz
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


--adding column day of week

SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone ,
convert_timezone('UTC', timezone, logs.datetime_iso8601 ) as game_event_ltz,
dayname(logs.datetime_iso8601) as DOW_NAME 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


--a Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

 --Check the table to see if loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;


SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAME_EVENT_UTC,
    tod_name,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(timezone) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod -- Assuming the correct table name is "TIME_OF_DAY_LU"
ON
    DATE_PART('hour', logs.datetime_iso8601) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int;



SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAME_EVENT_UTC,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(logs.datetime_iso8601) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod 
ON
    DATE_PART('hour', logs.datetime_iso8601) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int and
 USER_LOGIN ilike '%Prajina%';

--Wrap any Select in a CTAS statement
create or replace table ags_game_audience.enhanced.logs_enhanced as(
SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    tod_name,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(logs.datetime_iso8601) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod -- Assuming the correct table name is "TIME_OF_DAY_LU"
ON
    DATE_PART('hour', logs.datetime_iso8601) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
 --your select goes here
);

select * from ags_game_audience.enhanced.logs_enhanced 
select count(*) 
      from ags_game_audience.enhanced.logs_enhanced
      where dow_name = 'Sat'
      and tod_name = 'Early evening'   
      and gamer_name like '%prajina'
      
create or replace table ags_game_audience.enhanced.logs_enhanced as
(
SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
    DATE_PART('hour', game_event_ltz) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
);


grant execute task on account to role SYSADMIN;

xecute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

show tasks in account;

describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENCHANCED

execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--dumping all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then insert all in, to see the changes
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour);

INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod -- Assuming the correct table name is "TIME_OF_DAY_LU"
ON
    DATE_PART('hour', game_event_ltz) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int
);


--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;


MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
and r.datetime_iso8601=e.GAME_EVENT_UTC
and r.user_event=e.game_event_name
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

--let's truncate so we can start the load over again

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod 
ON
    DATE_PART('hour', game_event_ltz) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int) r

ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC=e.GAME_EVENT_UTC
and r.game_event_name=e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME)
VALUES
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME);

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';


elect * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 

select get_ddl('table','AGS_GAME_AUDIENCE.RAW.game_logs');

copy into PIPELINE_LOGS
from @ags_game_audience.raw.uni_kishore_pipeline
file_format=(format_name=ff_json_logs);

select * from LOGS_ENHANCED;

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod -- Assuming the correct table name is "TIME_OF_DAY_LU"
ON
    DATE_PART('hour', game_event_ltz) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int) r

ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC=e.GAME_EVENT_UTC
and r.game_event_name=e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME)
VALUES
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME);


create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES 
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as merge into ENHANCED.LOGS_ENHANCED e
USING (SELECT
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    tod_name,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(game_event_ltz) AS DOW_NAME
FROM
    AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN
    IPINFO_GEOLOC.demo.location loc
ON
    IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
JOIN
    AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod -- Assuming the correct table name is "TIME_OF_DAY_LU"
ON
    DATE_PART('hour', game_event_ltz) = tod.hour
AND
    IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN start_ip_int AND end_ip_int) r

ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC=e.GAME_EVENT_UTC
and r.game_event_name=e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME)
VALUES
(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, TOD_NAME, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME);


EXECUTE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;


list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;


select count(*) from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switching back to sysadmin
use role sysadmin;

USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL';



SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

   CREATE TABLE ED_PIPELINE_LOGS as 
   SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

  --truncate the table rows that were input during the CTAS
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


CREATE OR REPLACE PIPE GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;


show streams;


select system$stream_has_data('ed_cdc_stream');

--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');


select SYSTEM$PIPE_STATUS('GET_NEW_FILES');

--alter pipe GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe GET_NEW_FILES set pipe_execution_paused = false;

select * 
from ags_game_audience.raw.ed_cdc_stream; 

MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

        select * 
from ags_game_audience.raw.ed_cdc_stream; 



alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;


;

create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
    when system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
       
