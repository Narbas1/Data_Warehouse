# Data Warehouse

Simple data engineering project that collects cryptocurrency prices from the CoinGecko API and stores them in a layered data warehouse.

## Architecture

The project follows a Bronze / Silver / Gold structure:

- **Bronze** – Raw JSON data from the API
- **Silver** – Structured price data
- **Gold** – Latest price per coin

Pipeline flow:

CoinGecko API -> Bronze -> Silver -> Gold

---

## Tech Stack

- PostgreSQL
- Python
- Apache Airflow
- Docker

---

## What This Project Shows

- Incremental data loading
- Medallion architecture
- Airflow orchestration
- Snapshot table design for latest prices

---

## Note

This is a learning project and not production-ready.

It uses PostgreSQL (OLTP) as the data warehouse. 
That is ok for small datasets, but it would not scale well for large analytical workloads.

In real-world systems, OLAP database would be used instead.


