create server localredis foreign data wrapper redis_fdw;

create user mapping for public server localredis;

-- tables for all 5 data types (4 structured plus scalar)

create foreign table db15(key text, value text) 
       server localredis 
       options (database '15');

create foreign table db15h(key text, value text) 
       server localredis 
       options (database '15', tabletype 'hash');

create foreign table db15s(key text, value text) 
       server localredis 
       options (database '15', tabletype 'set');

create foreign table db15l(key text, value text) 
       server localredis 
       options (database '15', tabletype 'list');

create foreign table db15z(key text, value text) 
       server localredis 
       options (database '15', tabletype 'zset');

-- make sure they are all empty - if any are not stop the script right now

\set ON_ERROR_STOP 
do $$
  declare
    rows bigint;
  begin 
    select into rows 
        (select count(*) from db15) +
        (select count(*) from db15h) +
        (select count(*) from db15s) +
        (select count(*) from db15l) +
        (select count(*) from db15z);
    if rows > 0
    then
       raise EXCEPTION 'db 15 not empty';
    end if;
  end;
$$;
\unset ON_ERROR_STOP


-- ok, empty, so now run the setup script

\! redis-cli < test/sql/redis_setup

select * from db15;

-- hash

create foreign table db15hp(key text, value text) 
       server localredis 
       options (tabletype 'hash', tablekeyprefix 'hash', database '15');

create foreign table db15hpa(key text, value text[]) 
       server localredis 
       options (tabletype 'hash', tablekeyprefix 'hash', database '15');

create foreign table db15hsa(key text, value text[]) 
       server localredis 
       options (tabletype 'hash', tablekeyset 'hkeys', database '15');

select * from db15hp;

select * from db15hpa;

select * from db15hsa;

-- a couple of nifty things we an do with hash tables

select key, hstore(value) from db15hpa;

create type atab as (k1 text, k2 text, k3 text);

select key, (populate_record(null::atab, hstore(value))).* from db15hpa;

-- set

create foreign table db15sp(key text, value text) 
       server localredis 
       options (tabletype 'set', tablekeyprefix 'set', database '15');

create foreign table db15spa(key text, value text[]) 
       server localredis 
       options (tabletype 'set', tablekeyprefix 'set', database '15');

create foreign table db15ssa(key text, value text[]) 
       server localredis 
       options (tabletype 'set', tablekeyset 'skeys', database '15');

select * from db15sp;

select * from db15spa;

select * from db15ssa;

-- list

create foreign table db15lp(key text, value text) 
       server localredis 
       options (tabletype 'list', tablekeyprefix 'list', database '15');

create foreign table db15lpa(key text, value text[]) 
       server localredis 
       options (tabletype 'list', tablekeyprefix 'list', database '15');

create foreign table db15lsa(key text, value text[]) 
       server localredis 
       options (tabletype 'list', tablekeyset 'lkeys', database '15');

select * from db15lp;

select * from db15lpa;

select * from db15lsa;

-- zset

create foreign table db15zp(key text, value text) 
       server localredis 
       options (tabletype 'zset', tablekeyprefix 'zset', database '15');

create foreign table db15zpa(key text, value text[]) 
       server localredis 
       options (tabletype 'zset', tablekeyprefix 'zset', database '15');

create foreign table db15zsa(key text, value text[]) 
       server localredis 
       options (tabletype 'zset', tablekeyset 'zkeys', database '15');

select * from db15zp;

select * from db15zpa;

select * from db15zsa;

-- all done,so now blow everything in the db away agan

\! redis-cli < test/sql/redis_clean

