package operations;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import common.bigquery.BQTypeConstants;
import common.operations.GenericPrinter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class WriteJSONToBigQuery extends PTransform<PCollection<String>, PDone> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteJSONToBigQuery.class);

    private final String outputTable;
    private final String deadLetterTable;

    public WriteJSONToBigQuery(String outputTable, String deadLetterTable) {
        this.outputTable = outputTable;
        this.deadLetterTable = deadLetterTable;
    }

    @Override
    public PDone expand(PCollection<String> input) {

        WriteResult writeResult = input
                .apply("PrepareTableRows", MapElements.via(new JsonToTableRow()))
                .apply("InsertIntoBigQuery",
                BigQueryIO.writeTableRows()
                        .to(outputTable)
                        .withExtendedErrorInfo()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        WriteResult failedWrites = writeResult.getFailedInsertsWithErr()
                .apply("PrepareFailureTableRows", MapElements.via(new BQFailureAdder()))
                .apply(ParDo.of(new GenericPrinter<>()))
                .apply("WriteFailedInsertsToDLQ",
                        BigQueryIO.writeTableRows()
                                .withSchema(getFailureSchema())
                                .to(deadLetterTable)
                                .withExtendedErrorInfo()
                                .withoutValidation()
                                .ignoreUnknownValues()
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        failedWrites.getFailedInsertsWithErr().apply(ParDo.of(new GenericPrinter<>()));

        return PDone.in(input.getPipeline());
    }

    private static class BQFailureAdder extends SimpleFunction<BigQueryInsertError, TableRow> {
        @Override
        public TableRow apply(BigQueryInsertError bqInsertError) {

            // Extract
            TableRow r = bqInsertError.getRow();
            TableDataInsertAllResponse.InsertErrors error = bqInsertError.getError();

            // Log
            LOG.error("BigQuery parsed insert failing: {} reason: {}", r, error.toString());

            // Output
            return new TableRow()
                    .set("data", r.toString())
                    .set("failure_reason", error.toString());
        }
    }

    private static class JsonToTableRow extends SimpleFunction<String, TableRow> {

        @Override
        public TableRow apply(String input) {
            return convertJsonToTableRow(input);
        }

        public static TableRow convertJsonToTableRow(String json) {
            TableRow row;

            try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
                row = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + json, e);
            }

            return row;
        }
    }

    private static TableSchema getFailureSchema() {
        return new TableSchema().setFields(
                Arrays.asList(
                        new TableFieldSchema()
                                .setDescription("The failed table row")
                                .setName("data")
                                .setType(BQTypeConstants.STRING)
                                .setMode(BQTypeConstants.REQUIRED),
                        new TableFieldSchema()
                                .setDescription("Failure reason")
                                .setName("failure_reason")
                                .setType(BQTypeConstants.STRING)
                                .setMode(BQTypeConstants.REQUIRED)
                ));
    }
}
