# data_pipeline (dbt project)

dbt project that transforms the Snowflake TPC-H sample data into an order-level fact table. It is orchestrated by Airflow through Astronomer Cosmos; see the repository root README for the full setup.

## Models

- `staging/stg_tpch_orders`, `staging/stg_tpch_line_items`: views over `snowflake_sample_data.tpch_sf1`
- `marts/int_order_items`, `marts/int_order_items_summary`, `marts/fact_orders`: tables

## Commands

Run from this directory with a `data_pipeline` profile configured:

```bash
dbt deps     # install dbt_utils
dbt build    # run models and tests
dbt test     # run tests only
```

## Resources

- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [Astronomer Cosmos documentation](https://astronomer.github.io/astronomer-cosmos/)
