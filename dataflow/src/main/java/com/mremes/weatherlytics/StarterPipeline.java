package com.mremes.weatherlytics;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
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

public class StarterPipeline {

	static class StringToRawTableRowFn extends DoFn<String,TableRow> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			String timestamp = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now());
			c.output(new TableRow()
					.set("timestamp",timestamp)
					.set("data",c.element()));
		}
	}

	static class SplitInputStringFn extends DoFn<String,String> {
		@Override
		public void processElement(ProcessContext c) throws Exception {
			for(String s : c.element().split("\n")) {
				c.output(s);
			}
		}
	}

	static class InputToTableRows extends PTransform<PCollection<String>,PCollection<TableRow>> {
		@Override
        public PCollection<TableRow> apply(PCollection<String> input) {
            PCollection<String> lines = input.apply(ParDo.of(new SplitInputStringFn()));

            PCollection<TableRow> rows = lines.apply(ParDo.of(new StringToRawTableRowFn()));

            return rows;
        }
	}

	private static TableSchema getTableSchema() {
	    List<TableFieldSchema> fields = new ArrayList();
	    fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
	    fields.add(new TableFieldSchema().setName("data").setType("STRING"));

	    return new TableSchema().setFields(fields);
  }

	public static void main(String[] args) {
		PipelineOptionsFactory.register(BasicOptions.class);
		BasicOptions options = PipelineOptionsFactory.fromArgs(args)
			.withValidation()
			.create()
			.as(BasicOptions.class);

		Pipeline p = Pipeline.create(options);
		TableSchema schema = getTableSchema();

		String pubSubSubscription = String.format(
				"projects/%s/subscriptions/%s", options.getProjectId(), options.getSubscription());

		p.apply(PubsubIO
				.Read
				.named("ReadFromPubSub")
				.subscription(pubSubSubscription))
		.apply(new InputToTableRows())
		.apply(BigQueryIO.Write
				.named("WriteToBigQuery")
				.to(String.format("%s:%s.raw", options.getProjectName(), options.getDataSet()))
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		p.run();
  }
}
