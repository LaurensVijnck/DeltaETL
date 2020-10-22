package operations;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import common.bigquery.BQTypeConstants;
import common.operations.GenericPrinter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.Arrays;


/**
 * Created by Laurens on 20/10/20.
 *
 * Failsafe {@link PTransform} to write objects the BigQuery {@code OutputTable}. The transform is configured
 * by the {@code bqFormatFn} function that maps the input elements onto BigQuery rows. The transform assumes
 * that the destination table is already present. Failed inserts are routed to the {@code deadLetterTable}.
 */
public class WriteJSONToBigQuery<InputT> extends PTransform<PCollection<InputT>, PDone> {

    private final SimpleFunction<InputT, TableRow> bqFormatFn;
    private final String outputTable;
    private final String deadLetterTable;

    public WriteJSONToBigQuery(SimpleFunction<InputT, TableRow> bqFormatFn, String outputTable, String deadLetterTable) {
        this.bqFormatFn = bqFormatFn;
        this.outputTable = outputTable;
        this.deadLetterTable = deadLetterTable;
    }

    @Override
    public PDone expand(PCollection<InputT> input) {

        // Write elements using formatter
        WriteResult writeResult = input
                .apply(MapElements.via(bqFormatFn))
                .apply(BigQueryIO.writeTableRows()
                        .to(outputTable)
                        .withExtendedErrorInfo()
                        .withoutValidation()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        // Write failed inserts
        writeResult.getFailedInsertsWithErr()
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

        return PDone.in(input.getPipeline());
    }

    private static class BQFailureAdder extends SimpleFunction<BigQueryInsertError, TableRow> {
        @Override
        public TableRow apply(BigQueryInsertError error) {

            // Output
            return new TableRow()
                    .set("table_row", error.getRow().toString())
                    .set("failure_reason", error.getError().toString());
        }
    }

    private static TableSchema getFailureSchema() {
        return new TableSchema().setFields(
                Arrays.asList(
                        new TableFieldSchema()
                                .setDescription("The failed table row")
                                .setName("table_row")
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
