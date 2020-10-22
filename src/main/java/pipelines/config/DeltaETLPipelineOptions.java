package pipelines.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Created by Laurens on 20/10/21.
 */
public interface DeltaETLPipelineOptions extends DataflowPipelineOptions {

    @Description("Input directory")
    @Validation.Required
    @Default.String("gs://delta-etl/*")
    String getInputDirectory();
    void setInputDirectory(String value);

    @Description("Mutex bucket name")
    @Validation.Required
    @Default.String("gs://delta-etl-locks")
    String getMutexDirectory();
    void setMutexDirectory(String value);

    @Description("Directory poll interval in seconds")
    @Validation.Required
    @Default.Integer(30)
    Integer getPollIntervalSeconds();
    void setPollIntervalSeconds(Integer value);

    @Description("BigQuery output table")
    @Validation.Required
    @Default.String("geometric-ocean-284614:delta_etl.deltas")
    String getOutputTable();
    void setOutputTable(String value);

    @Description("BigQuery output dead-letter table")
    @Validation.Required
    @Default.String("geometric-ocean-284614:delta_etl.deltas_failed")
    String getDeadLetterTable();
    void setDeadLetterTable(String value);
}
