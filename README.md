# Azure 2019 Telemetry Explorer

## Overview

This repository contains the data processing pipeline, a machine learning model for invocation prediction, and a frontend for exploring the telemetry data. It focuses on:
- **Fleet Demand**: Visualizing the invocations per function over time.
- **Latency Analytics**: Spotting the high-latency outliers (p99 metrics) across applications.
- **Memory Efficiency**: Auditing memory allocations dynamically.
- **Invocation Prediction**: LightGBM binary classifier to predict whether a function will be invoked in the future.

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
   > **macOS note:** LightGBM requires OpenMP. Install it via Homebrew if you see a `libomp.dylib` error:
   > ```bash
   > brew install libomp
   > ```
4. Run the dashboard:
   ```bash
   streamlit run src/app.py
   ```
5. Run the prediction API:
   ```bash
   uvicorn src.api:app --reload
   ```

### Data Pipeline

Run the following scripts in order from the project root:

```bash
# 1. Ingest raw CSVs into SQLite
python src/ingest_data.py

# 2. Combine invocation tables into a sparse Parquet file
python src/combine_invocations.py

# 3. Train the invocation prediction model
python src/train_invocation_model.py
```

### Project Structure
- `src/app.py` - The main Streamlit dashboard layout.
- `src/api.py` - FastAPI application serving the LightGBM prediction model.
- `src/queries.py` - SQL queries targeting the SQLite database, cached for performance.
- `src/ingest_data.py` - Ingests raw Azure telemetry CSVs into a SQLite database.
- `src/combine_invocations.py` - Merges per-day invocation tables into a single sparse Parquet file.
- `src/train_invocation_model.py` - Trains a LightGBM model to predict future function invocations.
- `data/processed/` - Generated database and Parquet files (git-ignored).
- `models/` - Saved LightGBM model file (git-ignored).

## Data Source

Data is taken from the paper:
> Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider", in Proceedings of the 2020 USENIX Annual Technical Conference (USENIX ATC 20). USENIX Association, Boston, MA, July 2020.

The dataset can be found at [Azure/AzurePublicDataset](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md).