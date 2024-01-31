## Overview

This section provides a high-level overview of the project, including its objectives, target audience, and key features.

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

1. **Setup Docker Environment:**
    ## to set up the environment with docker use:
   docker-compose up -d 

![image](https://github.com/arpeggito/tft_project/assets/145495639/4c852e60-21e7-49f5-8abd-ac67ad7275ea)
