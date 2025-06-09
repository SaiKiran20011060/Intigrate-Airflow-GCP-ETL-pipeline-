**GCP Data Pipeline with Apache Airflow**

This project demonstrates an end-to-end data pipeline built using **Airflow**, integrated with **Google Cloud Platform** services such as **Cloud Storage (GCS)**, **BigQuery**, and **Cloud Data Fusion**.

📌 Overview

The pipeline performs the following steps:

1. **Data Generation**: A Python script generates synthetic employee data and stores it as a CSV file.
2. **Local to GCS Upload**: The generated CSV is uploaded from the local system to a specified GCS bucket.
3. **GCS to BigQuery Load**: The uploaded file is automatically loaded into a BigQuery table with schema autodetection.
4. **Cloud Data Fusion Trigger**: Finally, a Cloud Data Fusion pipeline is triggered to perform further ETL/ELT processing.
5. **Power BI**: At the end extracting the data from Google Big query via Power BI and visualizing the data.

All orchestration and scheduling is handled through **Airflow DAGs**.

---

## 🛠️ Tech Stack

- **Airflow**
- **Google Cloud Storage (GCS)**
- **Google BigQuery**
- **Google Cloud Data Fusion**
- **Python**
- **Power BI**
