package pipelines;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import operations.CSVFileToJSONConverter;
import operations.StreamingDocumentSource;
import operations.WriteJSONToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.DeltaETLPipelineOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Created by Laurens on 20/10/20.
 */
public class DeltaETL {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaETL.class);

    public static void main(String[] args) {

        // Parse arguments into options
        DeltaETLPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(DeltaETLPipelineOptions.class);

        // Create pipeline object
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("DiscoverDeltas",
                        new StreamingDocumentSource(
                            options.getInputDirectory(),
                            options.getMutexDirectory(),
                            options.getPollIntervalSeconds()))
                .apply("ConvertToJSON", ParDo.of(new CSVFileToJSONConverter(",")))
                .apply("WriteToBigQuery", new WriteJSONToBigQuery(options.getOutputTable(), options.getDeadLetterTable()));

        // Run pipeline
        pipeline.run();
    }
}
