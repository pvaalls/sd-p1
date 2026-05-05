-- psql -h localhost -U postgres -d concerts -f schema.sql

DROP TABLE IF EXISTS cd_un;

CREATE TABLE IF NOT EXISTS cd_un (
    seat_id INT PRIMARY KEY,
    client_id VARCHAR(9) NOT NULL
);
