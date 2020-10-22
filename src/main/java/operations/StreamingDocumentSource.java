package operations;


import com.google.common.io.ByteStreams;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.io.FilenameUtils;

/**
 * Created by Laurens on 20/10/20.
 */
public class StreamingDocumentSource extends PTransform<PBegin, PCollection<FileIO.ReadableFile>> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingDocumentSource.class);

    private final String inputDirectory;
    private final String mutexDirectory;
    private final Integer pollIntervalSeconds;

    public StreamingDocumentSource(String inputDirectory, String mutexDirectory, Integer pollIntervalSeconds) {
        this.inputDirectory = inputDirectory;
        this.mutexDirectory = mutexDirectory;
        this.pollIntervalSeconds = pollIntervalSeconds;
    }

    public PCollection<FileIO.ReadableFile> expand(PBegin input) {

        return input
                .apply("MatchFiles", FileIO.match()
                        .filepattern(inputDirectory)
                            .continuously(
                                Duration.standardSeconds(pollIntervalSeconds),
                                Watch.Growth.never()))
                .apply("ReadMatches", FileIO.readMatches())
                .apply("Deduplicate", ParDo.of(new GCSMutex(mutexDirectory)));
    }

    private static class GCSMutex extends DoFn<FileIO.ReadableFile, FileIO.ReadableFile> {

        private final String mutexDirectory;

        public GCSMutex(String mutexDirectory) {
            this.mutexDirectory = mutexDirectory;
        }

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile f, ProcessContext c) {

            String baseFileName = FilenameUtils.removeExtension(f.getMetadata().resourceId().getFilename());
            ResourceId newFileResourceId = FileSystems.matchNewResource(mutexDirectory + "/" + baseFileName + ".lock", false);

            try (ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
                 ReadableByteChannel readerChannel = Channels.newChannel(in);
                 WritableByteChannel writerChannel = FileSystems.create(newFileResourceId, MimeTypes.TEXT)) {

                ByteStreams.copy(readerChannel, writerChannel);
            } catch (Exception e) {

                LOG.info("Error creating {} {}", f.getMetadata().resourceId().getFilename(), e.getMessage());
                return;
            }

            c.output(f);
        }
    }
}
