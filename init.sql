CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow to airflow;
\c airflow;
GRANT ALL ON SCHEMA public TO airflow;

CREATE DATABASE metabase;
CREATE USER metabase WITH PASSWORD 'metabase';
GRANT ALL PRIVILEGES ON DATABASE metabase to metabase;
\c metabase;
GRANT ALL ON SCHEMA public TO metabase;

CREATE DATABASE chall_tft_stats;
CREATE DATABASE tft_challenger_leaderboard;
CREATE DATABASE arpeggito_stats;
