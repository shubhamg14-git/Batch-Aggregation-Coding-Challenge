# Batch-Aggregation-Coding-Challenge (PySpark)

This project is a PySpark batch job that reads time-series metric data from a CSV file and performs **aggregations** (average, min, max) grouped by **metric name** and **timeseries window**.

### Overview

- Input: A CSV file with metrics data (`metric`, `value`, `timestamp`)
- Output: Aggregated metrics written to CSV based on timeseries window passed during runtime ("15 minutes", "12 hours", "24 hours" etc)

### Input Data Format

The input CSV file should have the following columns:

| Metric        | Value | Timestamp                     |
|---------------|-------|-------------------------------|
| temperature   | 88    | 2022-06-04T12:01:00.000Z      |
| precipitation | 0.5   | 2022-06-04T14:23:32.000Z      |
| humidity      | 31.7  | 2022-06-04T15:24:32.000Z      |

### Output Format

| start_time      |end_time            | metric        | avg_value | min_value | max_value |
|-----------------|--------------------|---------------|-----------|-----------|-----------|
| 2022-06-04  0:00|2022-06-04 12:00    |temperature    | 88.50     | 88.0      | 89.0      |
| 2022-06-04 12:00|2022-06-04  0:00    |precipitation  | 0.50      | 0.5       | 0.5       |

### Requirements

- Python ≥ 3.7
- Apache Spark ≥ 3.5
- PySpark
- Java JDK ≥ 8
- pandas ≥ 1.3.5 (for PySpark CSV support)
- Git (for version control)

### Usage

1. Clone the Repository

```bash
git clone https://github.com/shubhamg14-git/Batch-Aggregation-Coding-Challenge.git
```


2. Place your input CSV

    Place your input (weather_data.csv) file in the input_data folder (Added a sample file)


3. Run requirements.txt to install required libraries

```bash
pip install -r requirements.txt
```

4. Run the PySpark Job

    This job can accept timeseries window parameter ("15 minutes", "12 hours", "24 hours" etc), if nothing is passed, it assumes window size of "24 hours"

```bash
spark-submit  job\batch_aggregate.py "12 hours"
```

4. Check the generated Output

    Check the output_data folder for the generated CSV (aggregated_weather_metrics.csv) with aggregated results.