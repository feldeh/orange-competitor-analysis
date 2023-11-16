# Belgian Telecom Competitor Analysis Pipeline

### Overview

This project automates the process of scraping, processing, and storing promotional data from competitors in the telecom industry. It focuses on two competitors: [Mobile Vikings](https://mobilevikings.be/en/) and [Scarlet](https://www.scarlet.be/en/homepage/), utilizing data extraction and loading it into BigQuery for further analysis.

### Architecture

- **Scrapers**: Extract data from Mobile Vikings and Scarlet websites.
- **Data Storage**: Scraped data is temporarily stored as JSON and then cleaned and converted to NDJSON format.
- **Data Warehouse**: Cleaned data is loaded into Google BigQuery for analytical purposes.
- **Technology Stack**: BeautifulSoup, Playwright, Docker, and Apache Airflow.

### Components

#### Scrapers

- Utilize BeautifulSoup and Playwright to navigate and extract data from competitor websites.
- Capture promotion-related data: product name, product category, URL, data allowance, minutes, SMS, upload/download speed, price, and scrape timestamp.
- The scraped data is stored in a JSON format for further processing.

#### Data Cleaning & Processing

- Clean and transform JSON data into NDJSON (newline delimited JSON).

#### Data Loading

- Using Google Cloud BigQuery Python Client, the cleaned data is loaded into specific BigQuery tables.
- Before loading, the data is compared with existing records from the table to prevent data duplication and ensuring that only new or modified records are inserted into BigQuery.

### Setup & Usage

#### Prerequisites

- Google Cloud Platform account
- Docker & Docker Compose

#### Installation

1. **Clone the Repository**
   ```sh
   git clone https://github.com/feldeh/telecom-competitor-analysis
   cd telecom-competitor-analysis
   ```
2. **Setup Google Cloud**

   - Setup a GCP Project, enable the BigQuery API, and create a Service Account with BigQuery Admin roles.
   - Download the JSON key file for the Service Account and place it in the gcloud directory as "bigquery_credentials.json".

3. **Setup Docker**

   - Ensure Docker and Docker Compose are installed on your system.
   - Initialize the database

   ```sh
   docker compose up airflow-init
   ```

   - Build the Docker image:

   ```sh
   docker-compose build
   ```

4. **Configure Airflow**

   - Define the scraping schedule and adjust configurations in the Airflow DAG.
   - Ensure any credentials, variables, and connections are securely set up in Airflow.

5. **Run the Pipeline**
   ```sh
   docker-compose up
   ```
   Navigate to Apache Airflow's UI from `localhost:8080` and enable the DAG.
