# README
To run this e2e:
- must have a `rearc_dev` catalog in your workspace
- must have databricks cli configured with `dev` profile
```bash
./setup.sh    # bootstrap workspace
./run.sh      # run full pipeline via DAB
./teardown.sh # cleanup after you
```

# Screenshots
![jobs.png](pipeline/screenshots/jobs.png)
![catalog_gold_tables.png](pipeline/screenshots/catalog_gold_tables.png)
![dlt_pipeline.png](pipeline/screenshots/dlt_pipeline.png)
![genie_space.png](pipeline/screenshots/genie_space.png)