package pipelines;

import com.google.api.services.bigquery.model.TableRow;
import models.FailSafeElement;
import models.coders.FailSafeElementCoder;
import operations.CSVFileToJSONConverter;
import operations.StreamingDocumentSource;
import operations.WriteJSONToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipelines.config.DeltaETLPipelineOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Created by Laurens on 20/10/20.
 *
 * Streaming {@link Pipeline} that monitors a given GCS path for CSV files
 * and stores them into BigQuery. CSV files are assumed to include a header file. The
 * pipeline does not manage the table schema, it should hence be created on beforehand.
 * The pipeline uses {@link FailSafeElement} objects to ensure traceability.
 *
 * Input elements are parsed to JSON according to the header line and
 * are 1-to-1 mapped to BigQuery rows. Valid events are pushed to the {@code OutputTable}
 * whereas invalid events are stored in the {@code DeadLetterTable}.
 *
 * The pipeline guarantees that each input file will be processed
 * exactly once by the use of a GCS-based locking strategy. Re-playing the input data
 * can be accomplished by supplying a new {@code MutexDirectory} in the {@link DeltaETLPipelineOptions}.
 *
 * UPDATE: After the discussion, it became apparent that per-element deduplication of the input
 * files was requested. As I quick test, I decided to add the mechanism based on a session-windowing. This guarantees
 * deduplication of events within the interval specified in the {@link DeltaETLPipelineOptions}.
 */
public class DeltaETL {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaETL.class);

    public static void main(String[] args) {

        // Parse arguments into options
        DeltaETLPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .create()
                .as(DeltaETLPipelineOptions.class);

        System.out.println(options.getInputDirectory());

        // Create pipeline object
        Pipeline pipeline = Pipeline.create(options);

        // Construct pipeline
        pipeline
                .apply("DiscoverDeltas",
                        new StreamingDocumentSource(
                            options.getInputDirectory(),
                            options.getMutexDirectory(),
                            options.getPollIntervalSeconds()))

                // Convert elements to JSON
                .apply("ConvertToJSON", ParDo.of(
                        new CSVFileToJSONConverter(",")))
                            .setCoder(FailSafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))

                // Add deduplication strategy
                .apply("KeyElements", WithKeys.of(new MD5KeyElements()))
                .apply("AssignSessionWindow", Window.into(
                        Sessions.withGapDuration(
                                Duration.standardSeconds(options.getDeduplicationIntervalSeconds()))))
                .apply(GroupByKey.create())
                .apply(MapElements.via(new OutputFirst<>()))

                // Write elements to BigQuery
                .apply("WriteToBigQuery",
                        new WriteJSONToBigQuery<>(
                                new FailSafeToTableRowConverter(),
                                options.getOutputTable(),
                                options.getDeadLetterTable()));


        // Run pipeline
        pipeline.run();
    }

    private static class OutputFirst<KeyT, ValueT> extends SimpleFunction<KV<KeyT, Iterable<ValueT>>, ValueT> {
        @Override
        public ValueT apply(KV<KeyT, Iterable<ValueT>> input) {
            for(ValueT value: input.getValue()) {
                return value;
            }

            return null; // Should not happen technically, as elements are data driven (i.e., window should contain atleast a single element)
        }
    }

    private static class MD5KeyElements implements SerializableFunction<FailSafeElement<String, String>, String> {

        @Override
        public String apply(FailSafeElement<String, String> input) {
            // Depending on the use-case, you could choose to exclude certain attributes from the hashing.
            return DigestUtils.md5Hex(input.getPayload());
        }
    }

    // Cut some corners with this one, normally an intermediate transform should be
    // responsible for the conversion/validation, and invalid events should be routed to a
    // deadLetter tupleTag. The deadletter tag, containing FailSafeElements, should be written to the DLQ table.
    private static class FailSafeToTableRowConverter extends SimpleFunction<FailSafeElement<String, String>, TableRow> {
        @Override
        public TableRow apply(FailSafeElement<String, String> input) {
            TableRow row = convertJsonToTableRow(input.getPayload());
            row.set("source", input.getOriginalPayload()); // This field should be handled by the deadLetter tag
            return row;
        }

        public static TableRow convertJsonToTableRow(String el) {
            TableRow row;

            try (InputStream inputStream = new ByteArrayInputStream(el.getBytes(StandardCharsets.UTF_8))) {
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + el, e);
            }

            return row;
        }
    }
}
