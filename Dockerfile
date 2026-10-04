FROM astrocrpublic.azurecr.io/runtime:3.3-8

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate
