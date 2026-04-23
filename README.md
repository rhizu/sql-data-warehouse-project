# Data Warehouse and Analytics Project
Building a modern data warehouse with SQL Server, including ETL processes, analytics and data modelling. This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights.

# Data Architecture
<img width="1125" height="763" alt="image" src="https://github.com/user-attachments/assets/70dadde4-ab3b-4350-aeb1-e08115d22c2b" />


- Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
- Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
- Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

## Project Overview
This project involves:
- Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
- ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
- Data Modeling: Developing fact and dimension tables optimized for analytical queries.
- Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

## Tools Used
- pgAdmin4: A popular and feature rich Open Source administration and development platform for PostgreSQL
- draw.io: Design data architecture, models, flows, and diagrams
- Python: To automate the process of loading csv files into the bronze layer
- Visual Studio Code: A free, lightweight, and highly popular open-source code editor developed by Microsoft

## Repository Structure
```
data-warehouse-project/
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
```
