# NYC Subway Ridership Forecasting Capstone Project

## Overview

This project implements an end-to-end predictive analytics pipeline to forecast NYC subway ridership using modern cloud-native tools and machine learning. The solution leverages automated data engineering, scalable infrastructure, and interactive visualizations to deliver actionable insights for transit planning and operations.

## Architecture
[Predictive Analytics Pipeline Architecture]

- **Cloud Run:** Automates data ingestion from NYC Subway data sources into the pipeline.
- **GCS Bucket:** Stores raw data as the initial landing zone.
- **DataProc (Apache Spark):** Performs validation and deduplication of raw data.
- **Delta Lake:** Organizes data using the Medallion Architecture (bronze, silver, gold layers) for progressive refinement and reliability.
- **BigQuery:** Acts as a feature store for analytics and model training.
- **XGBoost:** Trained time series model for ridership forecasting.
- **Looker Dashboard:** Interactive visualization of predictions, actuals, and trends.
- **GitHub Actions:** CI/CD automation for infrastructure provisioning and code deployment.
- **Cloud Logging:** Centralized monitoring and observability across all pipeline components.

## Methodology

1. **Cloud Infrastructure as Code:** All resources are provisioned and managed using Infrastructure as Code for consistency and repeatability.
2. **Automated ELT Pipeline:** Data flows through raw, validated, and curated layers following the Medallion Architecture.
3. **Advanced Time Series Modeling:** XGBoost model trained with engineered time-based features for accurate ridership forecasts.
4. **Deployment & Visualization:** Predictions are visualized in an interactive Looker dashboard for quick business insights.

## Project Structure

- `bronze_layer/` - Raw data ingestion scripts and configs
- `silver_layer/` - Data cleaning, validation, and transformation scripts
- `gold_layer/` - Feature engineering and curated datasets
- `modeling/` - Model training, evaluation, and inference scripts
- `forecasts/` - Generated forecasts and results
- `schemas/` - Data schemas and definitions
- `.github/workflows/` - GitHub Actions CI/CD workflows

## Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/krishnasurya1110/capstone-project.git
   ```
2. **Set up Google Cloud credentials and environment variables as specified in the project documentation.**
3. **Deploy infrastructure and pipeline using provided IaC scripts and GitHub Actions workflows.**
4. **Access the Looker dashboard for real-time insights.**

## Looker Dashboard

Explore the interactive dashboard for predictions and analytics:  
[NYC Subway Ridership Looker Dashboard](https://lookerstudio.google.com/u/0/reporting/c7d413fe-b8f0-4583-9e19-7a22e67e3a3f/page/tEnnC)

## Contributors

- [Krishnasurya Gopalakrishnan](https://github.com/krishnasurya1110)
- [Monica M](https://github.com/MonicaM680)




