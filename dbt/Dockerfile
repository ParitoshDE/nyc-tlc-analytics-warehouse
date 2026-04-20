FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core==1.11.* dbt-bigquery==1.11.*

WORKDIR /usr/app/dbt

COPY . .

ENTRYPOINT ["dbt"]
CMD ["run"]
