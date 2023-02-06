Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC

```sh
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "ETL web to gcs" --cron "0 5 1 * *" --timezone "UTC"
```

Output
```
Found flow 'etl-web-to-gcs'
Deployment YAML created at
'/Users/patrickwalukagga/Documents/andela/projects/data-eng-zoomcamp-2023/week_2_workflo
w_orchestration/prefect-zoomcamp/flows/home_work/etl_web_to_gcs-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass
--skip-upload to suppress this warning.
```

Apply deployment
```
prefect deployment apply etl_web_to_gcs-deployment.yaml
```

Output
```
Successfully loaded 'ETL web to gcs'
Deployment 'etl-web-to-gcs/ETL web to gcs' successfully created with id
'fc62f69a-0166-4ea7-9cc3-33b981577911'.
View Deployment in UI:
http://127.0.0.1:4200/deployments/deployment/fc62f69a-0166-4ea7-9cc3-33b981577911

To execute flow runs from this deployment, start an agent that pulls work from the
'default' work queue:
$ prefect agent start -q 'default'
```

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. 

```
prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow -n "ETL gcs to bq" --params='{"months":[2, 3], "year": 2019, "color": "yellow"}'
```

Apply deployment
```
prefect deployment apply etl_parent_flow-deployment.yaml
```

Get secret from secret block
```python
from prefect.blocks.system import Secret

secret_block = Secret.load("etl-web-to-gcs-secrets")

# Access the stored secret
secret_block.get()
```

How to make deployments with github block

- [Resource](https://www.prefect.io/guide/videos/video-whats-new-in-prefect-2-3-0/)