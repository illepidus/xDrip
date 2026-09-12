package com.eveningoutpost.dexdrip.diasync;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class DiasyncSensorEventEncoderTest {
    @Test
    public void encode_matchesDiasyncGoldenFixture() throws IOException {
        DiasyncSensorEvent event = new DiasyncSensorEvent(
                "SENSOR:550e8400-e29b-41d4-a716-446655440000",
                1789027200000L,
                123d,
                "libre-sensor-id",
                1.1d,
                -2d);

        String encoded = new DiasyncSensorEventEncoder().encode(event);

        assertThat(encoded).isEqualTo(readFixture().trim());
    }

    private static String readFixture() throws IOException {
        try (InputStream input = DiasyncSensorEventEncoderTest.class.getClassLoader()
                .getResourceAsStream("xdrip-event-v1/sensor.json")) {
            if (input == null) {
                throw new IOException("Missing sensor fixture");
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
