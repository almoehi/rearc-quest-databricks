# Summary

To run this e2e:
- must have a rearc_dev catalog in your WS
- must have databricks cli configured with "dev" profile
```bash
./setup.sh    # bootstrap workspace
./run.sh      # run full pipeline via DAB
./teardown.sh # cleanup after you
```

Total time: ~1day (incl. local setups, side quests and working through databricks free hickups)

## Trade-offs:
- should be using service principals (and separate UC perms) to manage deploys
- DLT pipeline does not support incremental loading of new data
  - certainly a requirement for a live project
- crawler performs format conversion for one source file to make ingested file format consistent - a "transformation" outside of the pipeline
  - for live project would integrate into the pipeline with proper testing
- state management in crawler is not ready for multi-worker / distributed loading
- data-masking to satisfy data privacy requirements not considered
  - sketched out some UC permissions to lock down schemas to org groups - but free edition seems to have some limits here
- potentially split pipeline into individual DLTs such that a failure in gold does not take down the entire pipeline
- potentially break out single gold table into per-series gold tables

## Architecture
  - bronze: unprocessed string data; this stage should never fail
  - silver: same tables as bronze, schema-on-read, basic normalizations, dedup
  - gold: single wide table of data with enrichments (which change rarely and assumed to be smallish); views for more constraint analytical question in the requirement
    - pyspark for better integration with python toolchain, for SQL prob. better to use DBT or sth
  - keep code simple, testable and composable with simple functions
  - testing strategy
    - unit tests
    - integration tests via databricks-sql-connector (couldn't get databricks-connect working in time)
  - uses DABs for declarative deployment and follow "everything-as-code" principle
  - deploys a Genie space with a "economic data analyst" agent

## Retrospective
- working around the limitations of Free Edition to get the target architecture & devX
- properly understanding the source data - would normally spend much more time on this

# General process
- Determine & define major system components
- Scaffold repository and project folders
  - claude assisted implementation: define requirements/deps/toolchain -> draft -> review/adjust -> approve
- Scaffold software architecture
  - modularization, interfaces, api contracts ... manually roughed into files
  - claude assisted implementation of specific logic based on requirements/instructions left as in-code comments -> review/adjust -> approve
- Prototype & iterate with local testing loop
  - actual business / pipeline / transformation logic based on requirements
  - claude driven: define -> write -> review -> revise -> approve loop
  - define & manually write out some important test conditions
    - claude implement the test logic
    - claude suggest & implement extended coverage
- Deploy dev
  - manual review & correctness verification of real data assets
- claude assisted troubleshooting and fixing errors (mostly databricks free related limitations)
- bells & whistles (CI/CD, permissions, ....)
  - claude assisted write -> review -> approve loop

## Hard dependencies
- databricks cli configured with some profile
- run pipeline/setup.sh for each catalog (dev, test, prod) for one-off provisioning (poor-man's IaaC)

## Design principles
- no Databricks notebooks in production
- apply "XXX-as-code" principle wherever possible
- tight dev loop with local / fail-fast testing
- medallion architecture for data pipelines
- data loading: incremental, multi-threaded, idempotent with (simple) state tracking of prev. ingested files

## High Level Design
Separation of concerns between data loading and processing.
Flexibility to have data loading deployed outside of Databricks for cost / scalability purposes.
Separate teams managing each code base independently.

### Crawler
- incremental, multi-threaded (IO-bound) and idempotent
- simple file-based persistent state management to track prev. ingested files
- categorize data files based on purpose & expected frequency of updates
  - mapping/enrichtment data, actual data files split into recent and historical/backfill
- downloads files into a given folder (can be extended to push to S3)
- structured file tree based on the types of data files
- inline format conversion of the single JSON mapping file into the majority TSV format - simplifies ingestion
- cli interface to run it for local dev

### Pipeline 
- two jobs:
  - Python job: run Crawler and land data into Databricks volume
  - DLT / SDP pipeline: for ingesting form volume and medallion processing
    - full-load DLT, not modeled for incremental updates (time-boxing)
- manually survey the data files to get an idea for:
  - formats / conversions / bugs & gotchas / checks that may be required
    - for the example sake: used claude to help surface them
    - this would normally be a very manual step I'd spend significant time on
- determine appropriate primary keys and use for data deduplication
