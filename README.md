# API Data Streaming Pipeline

A comprehensive real-time data streaming solution that ingests API data and processes it through a robust data engineering pipeline.

## Overview

This project demonstrates an end-to-end data engineering pipeline that ingests data from APIs, processes it in real-time, and stores it for analysis. The architecture implements a scalable, fault-tolerant system using modern data engineering tools and technologies.

The pipeline follows these key steps:

1. Fetch data from external APIs
2. Stream the data through Kafka
3. Process the streams with Spark
4. Store processed data in Cassandra
5. Orchestrate workflows with Airflow

## Technology Stack

- **Docker**: Containerization platform for consistent deployment across environments[^1]
- **Apache Airflow**: Workflow orchestration tool for scheduling and monitoring data pipelines[^1]
- **Apache Kafka**: Distributed streaming platform for high-throughput, fault-tolerant data ingestion[^1]
- **Apache Spark**: Unified analytics engine for large-scale data processing[^1]
- **Apache Cassandra**: NoSQL database for handling high-velocity data with no single point of failure[^1]
- **PostgreSQL**: Relational database for structured data storage and Airflow metadata[^1]


## Architecture



**Data Flow**:

- **Ingestion Layer**: External APIs → Kafka topics
- **Processing Layer**: Kafka → Spark Streaming
- **Storage Layer**: Processed data → Cassandra (NoSQL) and PostgreSQL (SQL)
- **Orchestration Layer**: Airflow manages the entire workflow[^1]


## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Internet connection (for API access)
- Basic understanding of data engineering concepts


## Project Structure

```
├── dags/
│   └── kafka_stream.py         # Airflow DAG for API data ingestion
├── scripts/
│   └── entrypoint.sh           # Container initialization script
├── docker-compose.yml          # Container orchestration
├── requirements.txt            # Python dependencies
├── spark_stream.py             # Spark streaming job
├── .env                        # Environment variables (git-ignored)
└── README.md                   # Project documentation
```


## Setup and Installation

### 1. Clone the repository

```bash
git clone https://github.com/shreyashreddyk/api-data-streaming.git
cd api-data-streaming
```


### 2. Install dependencies

```bash
pip install -r requirements.txt
```


### 3. Make the entrypoint script executable

```bash
chmod +x scripts/entrypoint.sh
```


### 4. Create .env file with your API credentials

```
API_KEY=your_api_key_here
API_SECRET=your_api_secret_here
```


### 5. Start the containers

```bash
docker-compose up -d
```

This command spins up the following containers:

- Zookeeper (configuration service for Kafka)
- Kafka Broker (message broker)
- Schema Registry (metadata service)
- Control Center (Kafka monitoring UI)
- Spark Master \& Worker (distributed processing)
- Cassandra (NoSQL database)
- Airflow Webserver \& Scheduler
- PostgreSQL (relational database)[^1]


## Running the Pipeline

### 1. Access Airflow UI

Navigate to `http://localhost:8080` in your browser to access the Airflow web interface.

### 2. Trigger the data ingestion DAG

In the Airflow UI, locate the `api_data_pipeline` DAG and click the "Play" button to trigger it. This will:

- Call external APIs to fetch data
- Stream the data into Kafka topics
- Schedule the data processing workflow[^1]


### 3. Monitor Kafka streams

Access the Kafka Control Center at `http://localhost:9021` to monitor topics, consumers, and data flow through the system.[^1]

### 4. Submit the Spark streaming job

```bash
spark-submit --master spark://localhost:7077 spark_stream.py
```

This starts the Spark streaming application that consumes data from Kafka, processes it according to business logic, and writes results to Cassandra.[^1]

### 5. Verify data in Cassandra

Connect to the Cassandra cluster and check that data is being stored correctly:

```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```

Then run queries to view your data:

```sql
DESCRIBE keyspaces;
USE api_data;
SELECT * FROM processed_data LIMIT 10;
```


## Monitoring

- **Spark UI**: Available at `http://localhost:9090`
- **Kafka Control Center**: Available at `http://localhost:9021`
- **Airflow Dashboard**: Available at `http://localhost:8080`


## Troubleshooting

### Common Issues

1. **Container startup failures**
    - Check container logs: `docker-compose logs [service_name]`
    - Verify that ports are not already in use
2. **API rate limiting**
    - Implement appropriate backoff strategies in your API calls
    - Check API response codes and handle accordingly
3. **Kafka connection issues**
    - Ensure Zookeeper is running properly
    - Check network connectivity between containers
4. **Spark job failures**
    - Review Spark UI for detailed error messages
    - Verify JAR dependencies are correctly specified

## References

1. Ganiyu, Y. (2023). "Realtime Data Engineering Project With Airflow, Kafka, Spark, Cassandra and Postgres." [Link](https://github.com/airscholar/e2e-data-engineering)
2. Apache Kafka Documentation: [https://kafka.apache.org/documentation/](https://kafka.apache.org/documentation/)
3. Apache Spark Documentation: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
4. Apache Airflow Documentation: [https://airflow.apache.org/docs/](https://airflow.apache.org/docs/)
5. Apache Cassandra Documentation: [https://cassandra.apache.org/doc/latest/](https://cassandra.apache.org/doc/latest/)

<div style="text-align: center">⁂</div>

[^1]: https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/30770294/b6be5455-b8d8-4e16-b0e4-21db09e108f7/Realtime-Data-Engineering-Project-With-Airflow-Kafka-Spark-Cassandra-and-Postgres-_-by-Yusuf-Ganiyu-Freedium.html

[^2]: https://github.com/shreyashreddyk/api-data-streaming?tab=readme-ov-file

[^3]: https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/30770294/b6be5455-b8d8-4e16-b0e4-21db09e108f7/Realtime-Data-Engineering-Project-With-Airflow-Kafka-Spark-Cassandra-and-Postgres-_-by-Yusuf-Ganiyu-Freedium.html

