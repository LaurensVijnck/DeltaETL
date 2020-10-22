package models;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import models.coders.FailSafeElementCoder;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Created by Laurens on 20/10/20.
 */
@EqualsAndHashCode
@Getter
@DefaultCoder(FailSafeElementCoder.class)
public class FailSafeElement<OriginalT, CurrentT> {

    private final OriginalT originalPayload;
    private final CurrentT payload;

    @Setter
    @Nullable private String failureReason;

    public FailSafeElement(OriginalT originalPayload, CurrentT payload) {
        this.originalPayload = originalPayload;
        this.payload = payload;
    }
}
