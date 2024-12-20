# **Employee Data Pipeline with SCD Implementation**

This project demonstrates a comprehensive ETL pipeline for managing employee data, featuring integration with **Apache NiFi**, **Amazon S3**, **Snowflake**, and SCD (Slowly Changing Dimensions) implementations. Below is the detailed flow and steps involved in the project.

---
## **Data Flow Diagram**
![Sample Image](https://github.com/bao040/Employee_data_processing_with_nifi_snowflake/blob/main/project_diagram.png)
## **Data Flow Overview**

### **1. Data Generation**
Employee data is generated dynamically using Python's **Faker** library. The data includes fields like `id`, `name`, `email`, `phone`, `address`, `dob`, `salary`, `department`, and more. The generated data is saved as a timestamped CSV file in the `employee_data` directory.

---

### **2. Data Ingestion with Apache NiFi**
- The generated CSV file is listed, fetched, and loaded into an **Amazon S3 bucket** using **Apache NiFi**.
- **NiFi** automates the data flow, ensuring the CSV files are ingested seamlessly into the S3 storage.
![Sample Image](https://github.com/bao040/Employee_data_processing_with_nifi_snowflake/blob/main/nifi_diagram.png)
---

### **3. Data Integration with Snowflake**
- The data in the S3 bucket is ingested into Snowflake using an **external stage** and **PIPE** configuration.
- **File Formats** and **Pipelines** are set up to manage and automate data ingestion into the Snowflake `employee_raw` table.

---

### **4. Snowflake Database Design**
- A database, `employee_nifi_scd`, and related tables are created:
  - **`employee_raw`**: Stores raw ingested data from S3.
  - **`employee`**: Holds current employee records with the latest updates.
  - **`employee_logs`**: Tracks historical records with SCD2 (Slowly Changing Dimension Type 2) logic.

---

### **5. SCD Implementation**
- **SCD1**: Updates the `employee` table for real-time changes.
- **SCD2**: Tracks historical changes in the `employee_logs` table with fields like `start_time`, `end_time`, and `is_current`.
- A Snowflake **procedure (`pdr_scd_demo`)** and **task (`tsk_scd_raw`)** handle the SCD1 implementation.
- A view, `v_employee_change_data`, identifies and manages data changes for SCD2.
- Another task, `tsk_scd2_hist`, updates the `employee_logs` table with historical changes.

---

### **6. Automation**
- The entire process is automated using Snowflake tasks scheduled to run periodically.
- Both SCD1 and SCD2 updates are triggered automatically to keep data consistent and up to date.

---

## **Technologies Used**
- **Python**: Used for generating synthetic employee data.
- **Apache NiFi**: Automates data ingestion from local files to Amazon S3.
- **Amazon S3**: Cloud storage for temporary CSV files.
- **Snowflake**: Serves as the data warehouse with SCD1 and SCD2 implementations.
- **Docker**: Provides containerization for reproducibility.
- **Faker Library**: Generates realistic dummy data.
- **SQL**: Implements Snowflake database tables, views, and procedures.
