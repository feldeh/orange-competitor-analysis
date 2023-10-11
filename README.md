# Automated Data Collection: Mobile Vikings and Scarlet

## Overview

This project was initiated by Orange and conducted by a team of data scientists from BeCode. The primary objective was to gather competitive intelligence from key competitors in the telecom industry, namely Mobile Vikings and Scarlet, to gain valuable insights into the telecom industry, helping Orange make informed decisions and stay competitive in the market. The target data included pricing, upload and download speeds, internet volume, and other relevant details.

### Data Collection

To achieve this, the team started by thoroughly examining the competitors' websites to identify the specific data elements to be collected. After gaining a comprehensive understanding of the data sources, we employed a combination of web scraping technologies to automate the data extraction process.

- **Beautiful Soup**: We used Beautiful Soup to parse the HTML structure of the websites and extract relevant information.

- **Playwright**: Playwright was instrumental in handling the interactive components of the websites, ensuring that we captured dynamic data accurately.

The collected data was then transformed into structured JSON format for further processing.

### Data Processing

At this point, data processing pipelines were introduced to the project. We use BigQuery to store the collected data and Airflow to automate data processing, allowing for regular updates of competitor data.

## Installation

