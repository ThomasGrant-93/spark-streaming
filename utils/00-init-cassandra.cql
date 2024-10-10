-- Cassandra
CREATE KEYSPACE stuff WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE stuff.weather
(
    uuid         UUID PRIMARY KEY,
    device       TEXT,
    temp DOUBLE,
    humd DOUBLE,
    pres DOUBLE,
    period_start TIMESTAMP,
    period_end   TIMESTAMP
);
