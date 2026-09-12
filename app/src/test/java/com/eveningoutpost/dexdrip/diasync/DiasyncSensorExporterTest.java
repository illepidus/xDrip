package com.eveningoutpost.dexdrip.diasync;

import static com.google.common.truth.Truth.assertThat;
import static org.robolectric.Shadows.shadowOf;

import android.content.Intent;
import com.eveningoutpost.dexdrip.RobolectricTestWithConfig;
import com.eveningoutpost.dexdrip.models.Calibration;
import com.eveningoutpost.dexdrip.models.Libre2RawValue;
import java.util.List;
import org.junit.Test;
import org.robolectric.RuntimeEnvironment;

public class DiasyncSensorExporterTest extends RobolectricTestWithConfig {
    @Test
    public void export_sendsExplicitSensorEventWithRawValueCalibration() {
        Libre2RawValue rawValue = rawValue();
        Calibration calibration = new Calibration();
        calibration.slope = 1.1d;
        calibration.intercept = -2d;

        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue, calibration);

        List<Intent> broadcasts = shadowOf(RuntimeEnvironment.application).getBroadcastIntents();
        assertThat(broadcasts).hasSize(1);
        Intent sent = broadcasts.get(0);
        assertThat(sent.getAction()).isEqualTo(DiasyncSensorExporter.ACTION_XDRIP_EVENT);
        assertThat(sent.getPackage()).isEqualTo(DiasyncSensorExporter.DIASYNC_PACKAGE);
        String payload = sent.getStringExtra(DiasyncSensorExporter.EXTRA_PAYLOAD);
        assertThat(payload).contains("\"rawValue\":123.0");
        assertThat(payload).contains("\"sensorId\":\"libre-sensor-id\"");
        assertThat(payload).contains(
                "\"calibration\":{\"slope\":1.1,\"intercept\":-2.0}");
    }

    @Test
    public void export_withoutCalibration_usesIdentityCalibration() {
        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue(), null);

        Intent sent = shadowOf(RuntimeEnvironment.application).getBroadcastIntents().get(0);
        assertThat(sent.getStringExtra(DiasyncSensorExporter.EXTRA_PAYLOAD)).contains(
                "\"calibration\":{\"slope\":1.0,\"intercept\":0.0}");
    }

    @Test
    public void export_withoutDiasyncInstalled_doesNotThrow() {
        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue(), null);

        assertThat(shadowOf(RuntimeEnvironment.application).getBroadcastIntents()).hasSize(1);
    }

    @Test
    public void export_withoutSavedRawValue_sendsNothing() {
        DiasyncSensorExporter.export(RuntimeEnvironment.application, null, null);

        assertThat(shadowOf(RuntimeEnvironment.application).getBroadcastIntents()).isEmpty();
    }

    @Test
    public void export_withoutSensorId_sendsNothing() {
        Libre2RawValue rawValue = rawValue();
        rawValue.serial = null;

        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue, null);

        assertThat(shadowOf(RuntimeEnvironment.application).getBroadcastIntents()).isEmpty();
    }

    @Test
    public void duplicateRawEvent_keepsStableEventIdAndPayload() {
        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue(), null);
        DiasyncSensorExporter.export(RuntimeEnvironment.application, rawValue(), null);

        List<Intent> broadcasts = shadowOf(RuntimeEnvironment.application).getBroadcastIntents();
        assertThat(broadcasts).hasSize(2);
        String firstPayload = broadcasts.get(0).getStringExtra(DiasyncSensorExporter.EXTRA_PAYLOAD);
        String duplicatePayload = broadcasts.get(1).getStringExtra(DiasyncSensorExporter.EXTRA_PAYLOAD);
        assertThat(duplicatePayload).isEqualTo(firstPayload);
        assertThat(firstPayload).contains(
                "\"eventId\":\"SENSOR:23040c3c89039a677137258a867266922cfa6de01f49c582a2381de58cde24ed\"");
    }

    private static Libre2RawValue rawValue() {
        Libre2RawValue rawValue = new Libre2RawValue();
        rawValue.timestamp = 1789027200000L;
        rawValue.glucose = 123d;
        rawValue.serial = "libre-sensor-id";
        return rawValue;
    }
}
