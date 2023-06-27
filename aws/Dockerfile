FROM python:3.9-slim-buster

# Instalação dos certificados SSL
RUN apt-get update && apt-get install -y ca-certificates

WORKDIR /usr/local/airflow

COPY airflow/dags /usr/local/airflow/dags
COPY requirements.txt requirements.txt

# Configuração dos certificados SSL para o pip
ENV SSL_CERT_DIR=/etc/ssl/certs
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org --upgrade pip \
    && pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -U setuptools wheel pipenv \
    && pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org -r requirements.txt

EXPOSE 3000
COPY entrypoint.sh /tmp/entrypoint.sh
ENTRYPOINT ["/tmp/entrypoint.sh"]
