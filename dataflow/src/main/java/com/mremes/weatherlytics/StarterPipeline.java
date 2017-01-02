/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.mremes.weatherlytics;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 */

public class StarterPipeline {
	public interface CustomOptions extends PipelineOptions {
		@Description("Target dataset in BigQuery.")
		String getDataSet();
		void setDataSet(String dataSet);
		
		@Description("Name of subscription to read data from.")
		String getSubscription();
		void setSubscription(String subscription);
		
		@Description("Project id in Google Cloud for subscription path.")
		String getProjectId();
		void setProjectId(String projectId);
		
		@Description("Project name in Google Cloud under which dataset is created.")
		String getProjectName();
		void setProjectName(String projectName);
	}
	
	static class StringToRawTableRow extends DoFn<String,TableRow> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			String timestamp = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now());
			c.output(new TableRow()
					.set("timestamp",timestamp)
					.set("data",c.element()));
		}
	}
	
	static class SplitInputString extends DoFn<String,String> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			for(String s : c.element().split("\n")) {
				c.output(s);
			}
		}
	}
	
	private static TableSchema getTableSchema() {
	    List<TableFieldSchema> fields = new ArrayList();
	    fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
	    fields.add(new TableFieldSchema().setName("data").setType("STRING"));
	    
	    return new TableSchema().setFields(fields);
  }

	public static void main(String[] args) {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions options = PipelineOptionsFactory.fromArgs(args)
			.withValidation()
			.create()
			.as(CustomOptions.class);
		
		Pipeline p = Pipeline.create(options);
		TableSchema schema = getTableSchema();

		String pubSubSubscription = String.format(
				"projects/%s/subscriptions/%s", options.getProjectId(), options.getSubscription());
    
		p.apply(PubsubIO
				.Read
				.named("ReadFromPubSub")
				.subscription(pubSubSubscription))
		.apply(ParDo.of(new SplitInputString()))
		.apply(ParDo.of(new StringToRawTableRow()))
		.apply(BigQueryIO.Write
				.named("WriteToBigQuery")
				.to(String.format("%s:%s.raw", options.getProjectName(), options.getDataSet()))
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		p.run();
  }
}
