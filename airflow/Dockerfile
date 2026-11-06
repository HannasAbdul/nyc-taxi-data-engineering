FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /requirements.txt

# Create an isolated Python environment for dbt
RUN python -m venv /home/airflow/dbt_venv

# Install the exact dbt versions used by the local project
RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.6" \
    "dbt-snowflake==1.12.4"