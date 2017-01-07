# Weatherflow Pipeline
This project is a data pipeline – from data fetching to loading to a data warehouse - using Python and [Google Cloud Platform](https://cloud.google.com/solutions/) products.

This project uses a simple Python script for calling a [Wunderground Weather API](https://www.wunderground.com/weather/api/d/docs?MR=1) and posting payload into a GCP's [Pub/Sub](https://cloud.google.com/pubsub/docs/) messaging broker for a [Google Dataflow](https://cloud.google.com/dataflow/docs/) job to consume. The Dataflow job then writes processed data into a BigQuery table (in a raw format, for now).

Right now project consists of:
- `api_fetcher/main.py` - Python script for fetching data from Wunderground Weather API and sending paylaod into a Pub/Sub topic
- `dataflow` - Dataflow jobs
- Google Cloud Platform services: [Pub/Sub](https://cloud.google.com/pubsub/docs/) for messaging and  [BigQuery](https://cloud.google.com/bigquery/docs/) to store the pipeline output


## Requirements
- [Cloud SDK](https://cloud.google.com/sdk/) set up
- Python 2.7.12 (for running the script)
- JDK 1.7 or later (for executing Dataflow jobs)
- [Pub/Sub client library for Python](https://cloud.google.com/pubsub/docs/reference/libraries#client-libraries-install-python)

Refer to [Dataflow's Quickstart documentation](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven) for setting up the Dataflow environment.

## Running Python script
Inside `api_fetcher` directory:
```
python main.py <wunderground_api_key> <pubsub_topic>
```

## Running Dataflow job
Inside the `dataflow` directory:
```
mvn compile exec:java -Dexec.mainClass=com.mremes.weatherlytics.StarterPipeline \
-Dexec.args="--dataSet=<bq_data_set> \
--subscription=<pubsub_subscription> \
--projectName=<project_name> \
--projectId=<project_id>"
--stagingLocation=<gcs_staging_location> \
--tempLocation=<gcs_temp_location>
```
