package com.mremes.weatherlytics;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public interface BasicOptions extends PipelineOptions {
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