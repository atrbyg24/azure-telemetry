# Azure 2019 Telemetry Explorer

A Streamlit dashboard built to visualize and explore the Azure Functions Dataset (2019). The application provides an interactive deep-dive into the serverless workloads characteristics, highlighting fleet demand, tail latency outliers, and resource efficiency.

## Overview

This repository contains the data processing pipeline and the frontend exploring the telemetry data. It focuses on:
- **Fleet Demand**: Visualizing the invocations per function over time.
- **Latency Analytics**: Spotting the high-latency outliers (p99 metrics) across applications.
- **Memory Efficiency**: Auditing memory allocations dynamically.

## Quick Start

### Installation

1. Clone this repository.
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the dashboard:
   ```bash
   streamlit run src/app.py
   ```

### Project Structure
- `src/app.py` - The main Streamlit dashboard layout.
- `src/queries.py` - SQL queries targeting the SQLite database, cached for performance.
- `src/ingest_data.py` - Logic used to process the raw Azure telemetry dataset into a clean DB.

## Data Source

Data is taken from the paper:
> Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", in Proceedings of the 2020 USENIX Annual Technical Conference (USENIX ATC 20). USENIX Association, Boston, MA, July 2020.

The dataset can be found at [Azure/AzurePublicDataset](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md).