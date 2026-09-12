package com.eveningoutpost.dexdrip.diasync;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class DiasyncCarbsEventEncoderTest {
    @Test
    public void encode_matchesDiasyncGoldenFixture() throws IOException {
        DiasyncCarbsEvent event = new DiasyncCarbsEvent(
                "CARBS:550e8400-e29b-41d4-a716-446655440002",
                1789027320000L,
                20d,
                "Afternoon snack");

        String encoded = new DiasyncEventEncoder().encode(event);

        assertThat(encoded).isEqualTo(readFixture("carbs.json").trim());
    }

    @Test
    public void description_isLimitedByUnicodeCodePoints() {
        String notes = " " + "🫐".repeat(300) + " ";

        String description = DiasyncCarbsExporter.descriptionFrom(notes);

        assertThat(description.codePointCount(0, description.length())).isEqualTo(256);
        assertThat(description.startsWith(" ")).isFalse();
        assertThat(description.endsWith(" ")).isFalse();
    }

    @Test
    public void description_replacesControlCharacters() {
        assertThat(DiasyncCarbsExporter.descriptionFrom(" Snack\nwith\tfruit "))
                .isEqualTo("Snack with fruit");
    }

    private static String readFixture(String name) throws IOException {
        try (InputStream input = DiasyncCarbsEventEncoderTest.class.getClassLoader()
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
