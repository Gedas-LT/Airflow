FROM astrocrpublic.azurecr.io/runtime:3.3-9-python-3.12
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY modules /usr/local/airflow/modules
