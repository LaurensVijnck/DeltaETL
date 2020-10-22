package operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.*;

import java.io.BufferedReader;
import java.nio.channels.Channels;
import java.util.*;

/**
 * Created by Laurens on 20/10/20.
 */
public class CSVFileToJSONConverter extends DoFn<FileIO.ReadableFile, String> {

    private final String delimiter;

    public CSVFileToJSONConverter(String delimiter) {
        this.delimiter = delimiter;
    }

    @ProcessElement
    public void processElement(@Element FileIO.ReadableFile f, ProcessContext c) {

        try (BufferedReader br = new BufferedReader(Channels.newReader(f.open(), "UTF-8"))) {

            String header = br.readLine();
            String line;

            while ((line = br.readLine()) != null) {
                c.output(CSVToJSON(header, line));
            }

        } catch (Exception e) {
            throw new RuntimeException("Error while parsing", e);
        }
    }

    private String CSVToJSON(String header, String fields) throws JsonProcessingException {
        List<String> columns = Arrays.asList(header.split(delimiter));
        List<String> values = Arrays.asList(fields.split(delimiter));
        Map<String, String> obj = new LinkedHashMap<>();

        // Construct JSON object
        for (int i = 0; i < columns.size(); ++i) {
            obj.put(columns.get(i), values.get(i));
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(obj);
    }
}
