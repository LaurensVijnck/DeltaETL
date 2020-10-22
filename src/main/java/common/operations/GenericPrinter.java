package common.operations;

import operations.StreamingDocumentSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Laurens on 20/10/20.
 */
public class GenericPrinter<InputT> extends DoFn<InputT, InputT> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingDocumentSource.class);

    @ProcessElement
    public void processElement(@Element InputT input, ProcessContext c) {
        LOG.info(input.toString());
        c.output(input);
    }
}
