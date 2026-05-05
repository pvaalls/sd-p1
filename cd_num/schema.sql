-- psql -h localhost -U postgres -d concerts -f schema.sql
-- docker exec -i my-postgres psql -U postgres -d concerts < backup.sql

DROP TABLE IF EXISTS cd_num;

CREATE TABLE IF NOT EXISTS cd_num (
    seat_id INT PRIMARY KEY,
    client_id VARCHAR(9) NOT NULL
);
