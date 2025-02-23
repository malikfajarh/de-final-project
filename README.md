# Fashion E-commerce Analytical and Monitoring Platform 
This platform is designed to integrate CSV format files by combining batch and streaming processing to create a single source of truth. The goal of the platform is to provide insight into customer transactions, product performance, business growth, and customer activity to support more accurate data analysis and more strategic decision-making.
<br />
<br />

![Image](https://github.com/user-attachments/assets/70a73178-7fa0-45be-9e71-5bae8b46d286)
<br />


# How to run:
1. Clone This Repo
2. Please download [transaction.csv](dags/resources/csv/download_transaction_csv.txt) and [click_activity.csv](kafka/csv/download_click_activity_csv.txt) follow the txt guidance
3. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user

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
