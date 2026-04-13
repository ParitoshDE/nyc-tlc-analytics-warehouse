FROM python:3.10-slim

RUN pip install --no-cache-dir dbt-core==1.7.* dbt-bigquery==1.7.*

WORKDIR /usr/app/dbt

COPY . .

ENTRYPOINT ["dbt"]
CMD ["run"]
