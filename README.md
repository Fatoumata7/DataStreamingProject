# **Spark Streaming**



## **Project Overview**

This project demonstrates a real-time streaming data pipeline using Kafka, Python, and Apache Spark (Scala). It simulates financial transactions, streams them through Kafka, and processes them with Spark Streaming to detect suspicious activities such as high-value transactions or unusual user behavior. It includes three main components:

1. A **Python app** [`producer.py`](python-kafka-app/producer.py).

This component implements a Kafka producer in Python that simulates a real-time stream of financial transactions. It generates between 10 and 100 transactions per second, with random yet realistic data including:
* Multiple users
* Varying transaction amounts
* Timestamps
* Geographic locations

The generated data is continuously pushed to a Kafka topic named `transactions`, making it suitable for testing, development, and demonstration of streaming data pipelines.

2. A **Spark/Scala app** [`MainApp.scala`](fraud-detection/src/main/scala/MainApp.scala)

This module is a Spark Streaming application written in Scala, responsible for real-time processing of transaction data.

It performs the following operations:

* Reads streaming data from the Kafka topic transactions
* Parses and transforms incoming JSON records into structured formats
* Applies real-time processing logic, including:
  * Flagging high-value transactions that exceed a configurable threshold (amount > 3500)
  * Detecting suspicious activity, such as more than 3 transactions from the same user within a 1-minute window
  * Identifying potentially fraudulent behavior, such as transactions occurring in multiple locations within a 2-minute time frame
  * Detecting suspicious behavior where a user initiates more than 3 small transactions (amount < 50) via PayPal within a 4-minute window.

* All suspicious events are then written to:
  * A Kafka topic named `fraud-alerts`
  * A Parquet file for long-term storage and analysis
  * The console, for real-time monitoring and debugging during development

3. Another **Python app** [`consumer.py`](python-kafka-app/consumer.py)

This component implements a Kafka consumer in Python that consumes and displays the fraud alerts from the Kafka topic fraud-alerts.



## **Architecture Diagram**

```plaintext
User -> [Python App] -> Kafka Topic (transactions) -> [Spark/Scala App] -> Kafka Topic (fraud-alerts) -> [Python App] -> User
```



## **Prerequisites**

1. **Install Scala and SBT**
   - [Install Scala](https://www.scala-lang.org/download/).
   - [Install SBT](https://www.scala-sbt.org/download.html).

2. **Install python requirements**:
   
   ```bash
   python -m pip install -r requirements.txt
   ```

## **Run the End-to-End Fraud Detection Project**

1. Start Kafka:
   ```bash
   kafka-start
   ```

2. Run the Python Producer:
   ```bash
   python producer.py
   ```

3. Run the Spark Processor:
   ```bash
   sbt package
   ```
   ```bash
   spark-env
   ```
   ```bash
   run-app
   ```

4. Run the Python Consumer:
   ```bash
   python consumer.py
   ```


---

## Team Members

- Pénélope MILLET
- Fatoumata WADIOU

---