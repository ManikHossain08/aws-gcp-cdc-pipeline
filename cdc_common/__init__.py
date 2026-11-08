"""Shared, cloud-agnostic building blocks used by Glue, transfer, BigQuery and Airflow code.

Kept free of pyspark / airflow imports so it can be packaged into every runtime
(Glue Spark, Glue Python Shell, Airflow workers) and unit-tested anywhere.
"""

__version__ = "1.0.0"
