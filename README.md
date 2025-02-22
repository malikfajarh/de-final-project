# Data Engineering Final Project
Fashion E-commerce analytical and monitoring platform with data processing batch and stream from csv files
<br />
<br />

How to run:
1. make sure installed git lfs by `git lfs install` for pulling large files
2. Clone This Repo.
3. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user.

4. ETL Batch
  - `make postgres`
  - `make airflow`
  

5. Streaming
 - recommended to stop the airflow container first for memorry efficiency!
  - `make kafka`
  - `make spark`
  - `make kafka-create-topic`
  - use 2 terminal for produce and consume messages!
  - `make kafka-produce` -> default is 100 for producing the messages and send to topic
  - `make kafka-consume` -> start kafka consumer listening to topic with timeout 10 seconds if no new data in topic

6. visualize with metabase
  - make sure files in scripts/metabase.sh is LF format
  - make sure postgres container is running and loaded with data from ETL and stream
  - `make metabase`

---
```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## docker-build-arm             - Build Docker Images (arm64) including its inter-container network.
## postgres                     - Run a Postgres container
## airflow                      - Spinup airflow scheduler and webserver.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## metabase                     - Spinup metabase instance.
## clean                        - Cleanup all running containers related to the challenge.
```
---