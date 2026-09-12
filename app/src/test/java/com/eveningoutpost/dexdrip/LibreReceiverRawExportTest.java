package com.eveningoutpost.dexdrip;

import static com.google.common.truth.Truth.assertThat;
import static org.robolectric.Shadows.shadowOf;

import android.content.Intent;
import android.os.Bundle;
import com.activeandroid.query.Delete;
import com.eveningoutpost.dexdrip.diasync.DiasyncSensorExporter;
import com.eveningoutpost.dexdrip.models.BgReading;
import com.eveningoutpost.dexdrip.models.Libre2RawValue;
import com.eveningoutpost.dexdrip.models.Sensor;
import com.eveningoutpost.dexdrip.utilitymodels.Intents;
import com.eveningoutpost.dexdrip.utilitymodels.Pref;
import com.eveningoutpost.dexdrip.utils.DexCollectionType;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.robolectric.RuntimeEnvironment;

public class LibreReceiverRawExportTest extends RobolectricTestWithConfig {
    @Before
    @Override
    public void setUp() {
        super.setUp();
        BgReading.deleteALL();
        Sensor.deleteAll();
        Libre2RawValue.updateDB();
        new Delete().from(Libre2RawValue.class).execute();
        DexCollectionType.setDexCollectionType(DexCollectionType.LibreReceiver);
    }

    @After
    public void tearDown() {
        BgReading.deleteALL();
        Sensor.deleteAll();
        new Delete().from(Libre2RawValue.class).execute();
        Pref.removeItem(DexCollectionType.DEX_COLLECTION_METHOD);
    }

    @Test
    public void firstRawValue_savesProcessedReadingBeforeCalculatingSlope() {
        long timestamp = System.currentTimeMillis() - 60_000L;

        new LibreReceiver().onReceive(
                RuntimeEnvironment.application,
                rawIntent(timestamp, 121d));
        Await.awaitAtMost(() -> {
            List<BgReading> readings = BgReading.latest(1);
            return readings != null
                    && readings.size() == 1
                    && diasyncBroadcasts().size() == 1;
        });

        BgReading processedReading = BgReading.latest(1).get(0);
        assertThat(processedReading.timestamp).isEqualTo(timestamp);
        assertThat(processedReading.raw_data).isEqualTo(121d);
        assertThat(processedReading.calculated_value_slope).isEqualTo(0d);
        assertThat(diasyncBroadcasts()).hasSize(1);
    }

    @Test
    public void consecutiveMinuteRawValues_exportOutsideFiveMinuteBgReadingGate() {
        long firstTimestamp = System.currentTimeMillis() - 2 * 60_000L;
        Sensor sensor = Sensor.create(firstTimestamp - 60_000L, "libre-sensor-id");
        BgReading recentProcessedReading = new BgReading();
        recentProcessedReading.sensor = sensor;
        recentProcessedReading.sensor_uuid = sensor.uuid;
        recentProcessedReading.uuid = "recent-processed-reading";
        recentProcessedReading.timestamp = System.currentTimeMillis();
        recentProcessedReading.raw_data = 999d;
        recentProcessedReading.calculated_value = 999d;
        recentProcessedReading.save();

        LibreReceiver receiver = new LibreReceiver();
        receiver.onReceive(RuntimeEnvironment.application, rawIntent(firstTimestamp, 121d));
        receiver.onReceive(RuntimeEnvironment.application, rawIntent(firstTimestamp + 60_000L, 122d));
        Await.awaitAtMost(() -> diasyncBroadcasts().size() == 2);

        List<Intent> broadcasts = diasyncBroadcasts();
        assertThat(broadcasts).hasSize(2);
        assertThat(payloads(broadcasts)).containsExactly(
                firstTimestamp + ":121.0",
                (firstTimestamp + 60_000L) + ":122.0");
        assertThat(Libre2RawValue.latestForGraph(10, 0)).hasSize(2);
        assertThat(BgReading.latest(10)).hasSize(1);
    }

    private static Intent rawIntent(long timestamp, double rawValue) {
        Bundle bleManager = new Bundle();
        bleManager.putString("sensorSerial", "libre-sensor-id");
        return new Intent(Intents.LIBRE2_BG)
                .putExtra("timestamp", timestamp)
                .putExtra("glucose", rawValue)
                .putExtra("bleManager", bleManager);
    }

    private static List<Intent> diasyncBroadcasts() {
        List<Intent> result = new ArrayList<>();
        for (Intent intent : shadowOf(RuntimeEnvironment.application).getBroadcastIntents()) {
            if (DiasyncSensorExporter.ACTION_XDRIP_EVENT.equals(intent.getAction())) {
                result.add(intent);
            }
        }
        return result;
    }

    private static List<String> payloads(List<Intent> broadcasts) {
        List<String> result = new ArrayList<>();
        for (Intent broadcast : broadcasts) {
            JsonObject payload = JsonParser.parseString(
                    broadcast.getStringExtra(DiasyncSensorExporter.EXTRA_PAYLOAD)).getAsJsonObject();
            result.add(payload.get("occurredAtEpochMillis").getAsLong()
                    + ":"
                    + payload.getAsJsonObject("sensor").get("rawValue").getAsDouble());
        }
        return result;
    }
}
