
# Intelligent Data Pipeline for Real-Time Financial Transactions

This project demonstrates a full data engineering pipeline for simulating, processing, and visualizing financial transaction data using open-source tools.

## Project Overview

- Simulates real-time financial transactions using Python
- Streams data to Kafka
- Consumes and processes the stream using Apache Spark Structured Streaming
- Stores output as Snappy Parquet files
- Visualizes processed data using DuckDB and Streamlit

## Technologies Used

- Python 3.9+
- Apache Kafka (via Docker)
- Apache Spark (Structured Streaming)
- Snappy Parquet
- DuckDB
- Streamlit
- Pandas, Plotly, PyArrow

## Project Structure

```
intelligent-data-pipeline/
├── docker-kafka-spark/         # Docker setup for Kafka & Zookeeper
├── jars/                       # Required JARs for Spark integrations
├── output/parquet/             # Output Snappy Parquet files
├── sample_transactions.csv     # Sample dataset (small version)
├── generate_transactions.py    # Transaction generator
├── kafka_producer.py           # Sends transactions to Kafka topic
├── spark_kafka_consumer.py     # Spark job for consuming and saving parquet
├── streamlit_dashboard.py      # Streamlit dashboard powered by DuckDB
└── README.md                   # Project documentation
```

## Setup & Execution

### 1. Clone Repository
```bash
git clone https://github.com/smita-chaudhari/intelligent-data-pipeline.git
cd intelligent-data-pipeline
```

### 2. Start Kafka with Docker
```bash
cd docker-kafka-spark
docker compose up -d
```

### 3. Generate Sample Transactions
```bash
python generate_transactions.py
```

### 4. Start Kafka Producer
```bash
python kafka_producer.py
```

### 5. Start Spark Consumer
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --jars jars/kafka-clients-3.5.0.jar \
  spark_kafka_consumer.py
```

### 6. Visualize Data in Streamlit
```bash
streamlit run streamlit_dashboard.py
```

Open your browser at: [http://localhost:8501](http://localhost:8501)

## Dashboard Features

- Transaction filtering by region
- Total transaction KPIs
- Bar chart by security type
- Pie chart of region distribution
- Pivot-style view of amount breakdowns

## Requirements

Install required dependencies:
```bash
pip install -r requirements.txt
# OR
pip install duckdb streamlit pandas pyarrow plotly
```

## .gitignore Highlights

```
__pycache__/
*.pyc
spark-warehouse/
output/parquet/_spark_metadata/
output/parquet/*.snappy.parquet
*.log
.env
```

## Screenshots

Screenshots are available under the `/assets` folder in the repo for a walkthrough of the dashboard.

## About the Author

Created by Smita Chaudhari to demonstrate practical knowledge of modern data engineering tools and workflow orchestration.
