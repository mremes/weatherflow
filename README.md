This project is a data pipeline – from data fetching to loading to a data warehouse - using Python and Google Cloud Platform products.

This project uses a simple Python script for calling a Wunderground Weather API and posting payload into a GCP's Pub/Sub messaging broker for a Google Dataflow (TBD: migrate to the Apache Beam SDK) job to consume. The Dataflow job then writes processed data into a BigQuery table.

Right now project consists of:
