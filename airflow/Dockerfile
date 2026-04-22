FROM quay.io/astronomer/astro-runtime:12.0.0

# Install Python dependencies for ingestion
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy ingestion script into the container
COPY scripts/scripts/ingest_citibike.py /usr/local/airflow/scripts/ingest_citibike.py

# Copy dbt project into the container
COPY . /usr/local/airflow/dbt/
