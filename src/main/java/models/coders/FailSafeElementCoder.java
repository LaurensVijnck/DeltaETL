package models.coders;

import lombok.AllArgsConstructor;
import lombok.Getter;
import models.FailSafeElement;
import org.apache.beam.sdk.coders.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Laurens on 20/10/20.
 */
@Getter
@AllArgsConstructor
public class FailSafeElementCoder<OriginalT, CurrentT> extends CustomCoder<FailSafeElement<OriginalT, CurrentT>> {

    private final Coder<OriginalT> originalPayloadCoder;
    private final Coder<CurrentT> currentPayloadCoder;
    private static final NullableCoder<String> nullableStringCoder = NullableCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(FailSafeElement<OriginalT, CurrentT> value, OutputStream outStream) throws CoderException, IOException {
        if (value == null) {
            throw new CoderException("Null object unsupported!");
        }

        originalPayloadCoder.encode(value.getOriginalPayload(), outStream);
        currentPayloadCoder.encode(value.getPayload(), outStream);
        nullableStringCoder.encode(value.getFailureReason(), outStream);
    }

    @Override
    public FailSafeElement<OriginalT, CurrentT> decode(InputStream inStream) throws CoderException, IOException {
        OriginalT originalPayload = originalPayloadCoder.decode(inStream);
        CurrentT currentPayload = currentPayloadCoder.decode(inStream);
        String failureReason = nullableStringCoder.decode(inStream);

        FailSafeElement failSafeElement = new FailSafeElement<>(originalPayload, currentPayload);
        failSafeElement.setFailureReason(failureReason);

        return failSafeElement;
    }

    public static <OriginalT, CurrentT> FailSafeElementCoder<OriginalT, CurrentT> of(
            Coder<OriginalT> originalPayloadCoder, Coder<CurrentT> currentPayloadCoder) {
        return new FailSafeElementCoder<>(originalPayloadCoder, currentPayloadCoder);
    }
}
