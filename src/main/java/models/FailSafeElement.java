package models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@EqualsAndHashCode
@Getter
@AllArgsConstructor
@DefaultCoder(AvroCoder.class)
public class FailSafeElement<OriginalT, CurrentT> {

    private final OriginalT originalPayload;
    private final CurrentT payload;
}
