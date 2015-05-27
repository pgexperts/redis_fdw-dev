
CREATE OR REPLACE FUNCTION atsort( a text[])
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE  STRICT
AS $function$
  select array(select unnest($1) order by 1)
$function$

;



create server localredis foreign data wrapper redis_fdw;

create user mapping for public server localredis;

-- tables for all 5 data types (4 structured plus scalar)

create foreign table db15(key text, value text)
       server localredis
       options (database '15');

create foreign table db15_hash(key text, value text)
       server localredis
       options (database '15', tabletype 'hash');

create foreign table db15_set(key text, value text)
       server localredis
       options (database '15', tabletype 'set');

create foreign table db15_list(key text, value text)
       server localredis
       options (database '15', tabletype 'list');

create foreign table db15_zset(key text, value text)
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
        (select count(*) from db15_hash) +
        (select count(*) from db15_set) +
        (select count(*) from db15_list) +
        (select count(*) from db15_zset);
    if rows > 0
    then
       raise EXCEPTION 'db 15 not empty';
    end if;
  end;
$$;
\unset ON_ERROR_STOP


-- ok, empty, so now run the setup script

\! redis-cli < test/sql/redis_setup

select * from db15 order by key;

select * from db15 where key = 'foo';

-- hash

create foreign table db15_hash_prefix(key text, value text)
       server localredis
       options (tabletype 'hash', tablekeyprefix 'hash', database '15');

create foreign table db15_hash_prefix_array(key text, value text[])
       server localredis
       options (tabletype 'hash', tablekeyprefix 'hash', database '15');

create foreign table db15_hash_keyset_array(key text, value text[])
       server localredis
       options (tabletype 'hash', tablekeyset 'hkeys', database '15');

select * from db15_hash_prefix order by key;
select * from db15_hash_prefix where key = 'hash1';

select * from db15_hash_prefix_array order by key;
select * from db15_hash_prefix_array where key = 'hash1';

select * from db15_hash_keyset_array order by key;
select * from db15_hash_keyset_array where key = 'hash1';

-- a couple of nifty things we an do with hash tables

select key, hstore(value) from db15_hash_prefix_array order by key;

create type atab as (k1 text, k2 text, k3 text);

select key, (populate_record(null::atab, hstore(value))).*
from db15_hash_prefix_array
order by key;

-- set

create foreign table db15_set_prefix(key text, value text)
       server localredis
       options (tabletype 'set', tablekeyprefix 'set', database '15');

create foreign table db15_set_prefix_array(key text, value text[])
       server localredis
       options (tabletype 'set', tablekeyprefix 'set', database '15');

create foreign table db15_set_keyset_array(key text, value text[])
       server localredis
       options (tabletype 'set', tablekeyset 'skeys', database '15');

-- need to use atsort() on set results to get predicable output
-- since redis will give them back in arbitrary order
-- means we can't show the actual value for db15_set_prefix which has it as a
-- single text field

select key, atsort(value::text[]) as value from db15_set_prefix order by key;
select key, atsort(value::text[]) as value from db15_set_prefix where key = 'set1';

select key, atsort(value) as value from db15_set_prefix_array order by key;
select key, atsort(value) as value from db15_set_prefix_array where key = 'set1';

select key, atsort(value) as value from db15_set_keyset_array order by key;
select key, atsort(value) as value from db15_set_keyset_array where key = 'set1';

-- list

create foreign table db15_list_prefix(key text, value text)
       server localredis
       options (tabletype 'list', tablekeyprefix 'list', database '15');

create foreign table db15_list_prefix_array(key text, value text[])
       server localredis
       options (tabletype 'list', tablekeyprefix 'list', database '15');

create foreign table db15_list_keyset_array(key text, value text[])
       server localredis
       options (tabletype 'list', tablekeyset 'lkeys', database '15');

select * from db15_list_prefix order by key;
select * from db15_list_prefix where key = 'list1';

select * from db15_list_prefix_array order by key;
select * from db15_list_prefix_array where key = 'list1';

select * from db15_list_keyset_array order by key;
select * from db15_list_keyset_array where key = 'list1';

-- zset

create foreign table db15_zset_prefix(key text, value text)
       server localredis
       options (tabletype 'zset', tablekeyprefix 'zset', database '15');

create foreign table db15_zset_prefix_array(key text, value text[])
       server localredis
       options (tabletype 'zset', tablekeyprefix 'zset', database '15');

create foreign table db15_zset_keyset_array(key text, value text[])
       server localredis
       options (tabletype 'zset', tablekeyset 'zkeys', database '15');

select * from db15_zset_prefix order by key;
select * from db15_zset_prefix where key = 'zset1';

select * from db15_zset_prefix_array order by key;
select * from db15_zset_prefix_array where key = 'zset1';

select * from db15_zset_keyset_array order by key;
select * from db15_zset_keyset_array where key = 'zset1';

-- singleton scalar

create foreign table db15_1key(value text)
       server localredis
       options (singleton_key 'foo', database '15');

select * from db15_1key;

-- singleton hash

create foreign table db15_1key_hash(key text, value text)
       server localredis
       options (tabletype 'hash', singleton_key 'hash1', database '15');

select * from db15_1key_hash order by key;


-- singleton set

create foreign table db15_1key_set(value text)
       server localredis
       options (tabletype 'set', singleton_key 'set1', database '15');

select * from db15_1key_set order by value;


-- singleton list

create foreign table db15_1key_list(value text)
       server localredis
       options (tabletype 'list', singleton_key 'list1', database '15');

select * from db15_1key_list order by value;


-- singleton zset

create foreign table db15_1key_zset(value text)
       server localredis
       options (tabletype 'zset', singleton_key 'zset1', database '15');

select * from db15_1key_zset order by value;


-- singleton zset with scores

create foreign table db15_1key_zset_scores(value text, score numeric)
       server localredis
       options (tabletype 'zset', singleton_key 'zset1', database '15');

select * from db15_1key_zset_scores order by score desc;





-- all done,so now blow everything in the db away agan

\! redis-cli < test/sql/redis_clean

