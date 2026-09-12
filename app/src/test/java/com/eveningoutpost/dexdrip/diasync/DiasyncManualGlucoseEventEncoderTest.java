package com.eveningoutpost.dexdrip.diasync;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class DiasyncManualGlucoseEventEncoderTest {
    @Test
    public void encode_matchesDiasyncGoldenFixture() throws IOException {
        DiasyncManualGlucoseEvent event = new DiasyncManualGlucoseEvent(
                "MANUAL_GLUCOSE:550e8400-e29b-41d4-a716-446655440001",
                1789027260000L,
                121d);

        String encoded = new DiasyncEventEncoder().encode(event);

        assertThat(encoded).isEqualTo(readFixture("manual-glucose.json").trim());
    }

    private static String readFixture(String name) throws IOException {
        try (InputStream input = DiasyncManualGlucoseEventEncoderTest.class.getClassLoader()
                .getResourceAsStream("xdrip-event-v1/" + name)) {
            if (input == null) {
                throw new IOException("Missing fixture " + name);
            }
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int count;
            while ((count = input.read(buffer)) != -1) {
                output.write(buffer, 0, count);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
