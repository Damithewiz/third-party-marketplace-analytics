# Third-Party Marketplace Analytics Platform

### End-to-End Data Engineering, Analytics & Business Intelligence

An end-to-end data engineering and analytics project designed to transform third-party marketplace data into reliable, analysis-ready datasets for commercial reporting, partner performance monitoring and product health analysis.

Built using **Databricks, PySpark, Delta Lake, Unity Catalog and Power BI**.

## 1. Project Overview

Third-party marketplaces generate large volumes of operational data across products, sellers, customers, orders, payments and customer reviews.

The challenge is integrating these datasets into a consistent, reliable structure that enables business users to understand commercial performance, identify operational issues and make data-driven decisions.

This project demonstrates how external marketplace data can be ingested, standardised, validated and transformed into a curated analytics layer.

## 2. Business Objectives

- Centralise third-party marketplace data into a structured analytics platform.
- Standardise external data and establish relationships between customers, products, sellers and transactions.
- Enable analysis of revenue, orders, product performance and partner performance.
- Improve data quality through validation and transformation rules.
- Provide a foundation for predictive product health monitoring.
- Deliver business insights through an interactive Power BI report.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data engineering and processing environment |
| PySpark | Data ingestion and transformations |
| Delta Lake | Storage format for analytical tables |
| Unity Catalog | Data organisation and governance |
| Python | Data processing and analytical development |
| Power BI | Semantic modelling and business intelligence reporting |

## 4. Data Architecture

The project follows a Medallion Architecture:

**Landing → Bronze → Silver → Gold → Power BI**

**Landing Layer:** Stores external CSV files before transformation.

**Bronze Layer:** Ingests raw source data into Delta tables, preserving source-level information.

**Silver Layer:** Cleans and standardises datasets, applies data quality rules and prepares consistent business entities.

**Gold Layer:** Produces dimensional and fact tables designed for business reporting and downstream analytics.

**Validation:** Checks Gold table row counts and duplicate dimension keys and fact grains.

## 5. Data Sources

The project uses the public Olist Brazilian E-Commerce dataset, containing information about:

- Customers
- Sellers
- Products
- Orders
- Order items
- Payments
- Reviews
- Product category translations

Source: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## 6. Gold Data Model

The Gold layer contains eight analytical tables.

**Dimensions**
- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_seller`

**Facts**
- `fact_orders`
- `fact_order_items`
- `fact_payments`
- `fact_reviews`

These tables provide the foundation for commercial analysis, partner reporting, customer experience monitoring and product-level analytics.

## 7. Data Quality and Validation

Data validation is incorporated into the engineering process to improve reliability and identify potential issues before reporting.

The Gold validation notebook includes:

- Table inventory and review
- Row-count checks
- Duplicate dimension-key checks
- Duplicate fact-grain checks

Additional operational quality checks can be introduced as the platform develops.

## 8. Business Intelligence Report

The Power BI report is designed around three connected reporting pages:

1. **Executive Overview** — Revenue, orders, commercial KPIs and operational performance.
2. **Product Performance** — Product and category performance, revenue and sales volume.
3. **Customer Experience** — Customer reviews, satisfaction and delivery performance.

## 9. Planned AI Product Health Capability

The next development phase will introduce machine learning to estimate the probability of poor customer reviews.

Predictions will be combined with operational and product quality indicators to support prioritisation of products requiring investigation.

The resulting Gold output will be integrated into the Power BI semantic model.

## 10. Workflow Automation

The project is being developed towards an orchestrated Databricks workflow that coordinates ingestion, transformations and validation.

The intended workflow will support repeatable processing and provide a foundation for incremental loading and operational monitoring.

## 11. Repository Structure

```text
notebooks/
  00_project_setup.py
  01_data_ingestion.py
  02_bronze_ingestion.py
  03_silver_transformations.py
  04_gold_transformations.py
  05_gold_validation.py

README.md
```

Further documentation, workflow configuration, AI notebooks and Power BI deliverables will be added as development progresses.

## 12. Project Status

| Component | Status |
|---|---|
| Project setup | Completed |
| Data ingestion | Completed |
| Bronze transformations | Completed |
| Silver transformations | Completed |
| Gold dimensional modelling | Completed |
| Gold validation | Completed |
| AI Product Health | In development |
| Workflow automation | Planned |
| Power BI semantic model | Planned |
| Power BI dashboard | Planned |

## 13. Future Enhancements

- Introduce incremental ingestion and Delta MERGE processing.
- Orchestrate pipeline execution using Databricks Workflows.
- Implement predictive product health scoring.
- Build the Power BI semantic model and five-page report.
- Add pipeline monitoring and data quality logging.
- Document business findings and operational recommendations.
