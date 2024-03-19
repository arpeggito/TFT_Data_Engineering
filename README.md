## TFT Leaderboard and Player Analysis with Docker, PostgreSQL, Airflow, and Python

## Overview

This section provides a high-level overview of the project, including its objectives, target audience, and key features.

![image](https://github.com/arpeggito/tft_project/assets/145495639/4c852e60-21e7-49f5-8abd-ac67ad7275ea)

## Project Goals

- Create a real-time leaderboard of the top TFT players on the EUW server
- Gather the last 10 games for each top player for further data analysis
- Develop a data pipeline using Docker, PostgreSQL, Airflow, and Python

## Tools Used 

- **Docker:** Used for containerization and managing the infrastructure of the project.
- **Postgres:** The chosen relational database for storing both the tools' metadata and the game data.
- **Airflow:** Utilized for scheduling and orchestrating the data pipelines, ensuring seamless and timely execution.
- **Python:** The primary language for developing data pipelines, handling API requests, and performing data analysis.

## Prerequisites
To use the template, please install the following.

1. git
2. Github account
3. Docker with at least 4GB of RAM and Docker Compose v1.27.0 or later

## How to Run
**Setup Docker Environment:**
1. to set up the environment with docker use:
   docker-compose up -d
   After running the docker-compose, run "docker ps" to see if the containers are up and running:
   ![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/f6ea97ef-1a6c-4cc7-9234-32c07b344e7b)


2. Make sure to create your .env file with the required variables to be able to run the script
3. the init.sql file runs the first time the database is created to give the Metabase and airflow databases the required privileges (Yes we migrating the SQLite databases from the tools to Postgres)
4. The docker-compose file will bring up all the services, Metabase, Airflow, and Postgres

**Accessing the tools:**

1. Airflow

![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/bb9c7d70-2943-4cc3-bb0f-4473d5bfd7cb)

2. Snowflake / Postgres:

![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/2e83f8ad-596f-4933-9246-cfc0cef46aac)

![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/1505a2e7-22c9-411e-83a0-b93342889e7f)


Since visualizing data in the terminal for Postgres is a bit messy, I just rather stick with Snowflake since it's more than enough.

4. Metabase (Data Visualization)

![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/7fb2dcae-a699-4e43-ac12-f547029e984f)

Brose Data:
- **TFT Leaderboard**
![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/24495f0a-698b-4068-b9e9-a8221babfa8c)

- **Last 10 TFT Challenger Matches of each Player**
![image](https://github.com/arpeggito/TFT_Data_Engineering/assets/145495639/c66c46d8-4bf0-4871-b802-5d0f5d512a7f)


