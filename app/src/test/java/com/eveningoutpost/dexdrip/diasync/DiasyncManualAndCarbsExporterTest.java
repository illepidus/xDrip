package com.eveningoutpost.dexdrip.diasync;

import static com.google.common.truth.Truth.assertThat;
import static org.robolectric.Shadows.shadowOf;

import android.content.Intent;
import com.eveningoutpost.dexdrip.RobolectricTestWithConfig;
import com.eveningoutpost.dexdrip.models.BloodTest;
import com.eveningoutpost.dexdrip.models.JoH;
import com.eveningoutpost.dexdrip.models.Treatments;
import java.util.List;
import org.junit.Test;
import org.robolectric.RuntimeEnvironment;

public class DiasyncManualAndCarbsExporterTest extends RobolectricTestWithConfig {
    @Test
    public void bloodTestCreate_manualEntry_sendsManualGlucose() {
        BloodTest.create(JoH.tsl() - 1_000L, 121d, "Manual Entry",
                "550e8400-e29b-41d4-a716-446655440001");

        String payload = onlyPayload();
        assertThat(payload).contains("\"eventId\":\"MANUAL_GLUCOSE:550e8400-e29b-41d4-a716-446655440001\"");
        assertThat(payload).contains("\"manualGlucose\":{\"mgdl\":121.0}");
        assertThat(payload).doesNotContain("calibration");
    }

    @Test
    public void bloodTestCreate_calibrationSource_sendsNothing() {
        BloodTest.create(JoH.tsl() - 2_000L, 121d, "Initial Calibration");

        assertThat(broadcasts()).isEmpty();
    }

    @Test
    public void bloodTestCreate_bluetoothMeterSource_sendsNothing() {
        BloodTest.create(JoH.tsl() - 3_000L, 121d, "Bluetooth Glucose Meter:\nDevice");

        assertThat(broadcasts()).isEmpty();
    }

    @Test
    public void treatmentCreate_insulinOnly_sendsNothing() {
        Treatments.create(0d, 2d, JoH.tsl() - 4_000L);

        assertThat(broadcasts()).isEmpty();
    }

    @Test
    public void treatmentCreate_carbsAndInsulin_sendsOnlyCarbs() {
        Treatments treatment = Treatments.create(
                20d,
                2d,
                JoH.tsl() - 5_000L,
                "550e8400-e29b-41d4-a716-446655440002");

        String payload = onlyPayload();
        assertThat(payload).contains("\"eventId\":\"CARBS:" + treatment.uuid + "\"");
        assertThat(payload).contains("\"carbs\":{\"grams\":20.0}");
        assertThat(payload).doesNotContain("insulin");
    }

    @Test
    public void carbsExporter_mapsSanitizedNotesToDescription() {
        Treatments treatment = new Treatments();
        treatment.uuid = "550e8400-e29b-41d4-a716-446655440002";
        treatment.timestamp = 1789027320000L;
        treatment.carbs = 20d;
        treatment.notes = " Afternoon\nsnack ";

        DiasyncCarbsExporter.export(RuntimeEnvironment.application, treatment);

        assertThat(onlyPayload()).contains("\"description\":\"Afternoon snack\"");
    }

    private static String onlyPayload() {
        assertThat(broadcasts()).hasSize(1);
        return broadcasts().get(0).getStringExtra(DiasyncEventTransport.EXTRA_PAYLOAD);
    }

    private static List<Intent> broadcasts() {
        return shadowOf(RuntimeEnvironment.application).getBroadcastIntents();
    }
}
