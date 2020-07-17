CREATE KEYSPACE IF NOT EXISTS demo;

CREATE TABLE demo.users (
    email text PRIMARY KEY,
    firstname text,
    lastname text,
    address jsonb
) WITH default_time_to_live = 0
    AND transactions = {'enabled': 'true'};