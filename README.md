# Ecommerce-Sales-Data-Processing

## Overview
This project processes and analyzes ecommerce sales data to generate insights and ensure data quality. It includes steps for data cleaning, null value handling, and reporting.

## Features
- Cleans sales and product data
- Checks for nulls in key columns and fills with default values
- Issues warnings for missing data
- Summarizes data quality issues

## Data Sources
- Customer data (xlsx)
- Product data (csv)
- Orders data (json)

## Key Columns Checked for Nulls
- Sales: `Customer Name`
- Products: `Product Name`, `Category`, `Sub-Category`, `Price per product`

## Null Handling Strategy
- Missing `Customer Name` filled with `"Unknown"`
- Missing `Product Name`, `Category`, `Sub-Category` filled with `"Unknown-sub-category"`
- Missing `Price per product` filled with `0`
- All nulls are reported in the summary

## Usage
1. Clone the repository.
2. Install required dependencies:
   bash
   pip install -r requirements.txt
   
3. Run the main processing notebook:
  /Ecommerce-Sales-Data-Processing/notebook/e_commerce_sales_data_analysis