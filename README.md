# ETL Data Pipeline Project

End-to-end ETL pipeline built using PySpark and AWS for scalable, production-ready data processing.

## Architecture

The pipeline follows a **Bronze–Silver–Gold** layered architecture:

- **Bronze Layer:**  
  Raw CSV data ingestion from multiple sources into a centralized repository. Handles missing values, type casting, and initial data validation.
  
- **Silver Layer:**  
  Cleansed, validated, and normalized Parquet data. Includes deduplication, null handling, and date/format standardization. Optimized for downstream analytics.

- **Gold Layer:**  
  Aggregated and analytics-ready CSV data. Metrics include total sales, total orders, average price per product/store/category. Can be directly consumed by BI tools or reporting systems.

## Tech Stack

- **Data Processing:** PySpark  
- **Storage:** AWS S3 (Bronze, Silver, Gold layers)  
- **Serverless Automation:** AWS Lambda for triggering ETL on S3 uploads  
- **Monitoring:** AWS CloudWatch logs and alerts  
- **CI/CD:** GitHub Actions for automated testing and deployment  
- **Local Development:** Python 3.x, Pandas, and PySpark  

## Features

- **Automated ETL Execution:**  
  Lambda triggers the Gold layer pipeline whenever new Silver layer data is uploaded to S3.
  
- **Data Quality & Validation:**  
  Ensures schema consistency, handles nulls, deduplicates records, and validates date and numeric formats.

- **Scalable Architecture:**  
  Bronze–Silver–Gold layers enable incremental updates and separation of raw vs processed vs analytics-ready data.

- **Single CSV Output for Analytics:**  
  Gold layer produces a single, consolidated CSV ready for BI tools, dashboards, or reporting pipelines.

- **CI/CD Integration:**  
  Full GitHub Actions workflow to push, test, and deploy ETL scripts and Lambda functions automatically.

- **Cloud Monitoring & Logging:**  
  Tracks execution status and errors in CloudWatch, enabling quick debugging and alerting.

