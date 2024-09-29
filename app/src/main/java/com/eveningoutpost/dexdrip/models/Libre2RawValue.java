package com.eveningoutpost.dexdrip.models;


import android.content.Intent;
import android.provider.BaseColumns;
import android.util.Log;

import androidx.preference.PreferenceManager;

import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.Table;
import com.activeandroid.query.Delete;
import com.activeandroid.query.Select;
import com.eveningoutpost.dexdrip.utilitymodels.Constants;
import com.eveningoutpost.dexdrip.xdrip;

import java.util.Date;
import java.util.List;

@Table(name = "Libre2RawValue2", id = BaseColumns._ID)
public class Libre2RawValue extends PlusModel {
    private static final String TAG = "Libre2RawValue";

    static final String[] schema = {
            "DROP TABLE Libre2RawValue;",
            "CREATE TABLE Libre2RawValue2 (_id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, serial STRING, glucose REAL);",
            "CREATE INDEX index_Libre2RawValue2_ts on Libre2RawValue2(ts);"
    };

    @Column(name = "serial", index = true)
    public String serial;

    @Column(name = "ts", index = true)
    public long timestamp;

    @Column(name = "glucose", index = false)
    public double glucose;

    public static List<Libre2RawValue> weightedAverageInterval(long min) {
        double timestamp = (new Date().getTime()) - (60000 * min);
        return new Select()
                .from(Libre2RawValue.class)
                .where("ts >= " + timestamp)
                .orderBy("ts asc")
                .execute();
    }

    public static List<Libre2RawValue> latestForGraph(int number, double startTime) {
        return latestForGraph(number, (long) startTime, Long.MAX_VALUE);
    }

    public static List<Libre2RawValue> latestForGraph(int number, long startTime) {
        return latestForGraph(number, startTime, Long.MAX_VALUE);
    }

    public static List<Libre2RawValue> latestForGraph(int number, long startTime, long endTime) {
        return new Select()
                .from(Libre2RawValue.class)
                .where("ts >= " + Math.max(startTime, 0))
                .where("ts <= " + endTime)
                .where("glucose != 0")
                .orderBy("ts desc")
                .limit(number)
                .execute();
    }

    public static List<Libre2RawValue> cleanup(final int retention_days) {
        updateDB();
        return new Delete()
                .from(Libre2RawValue.class)
                .where("ts < ?", JoH.tsl() - (retention_days * Constants.DAY_IN_MS))
                .execute();
    }

    public static void updateDB() {
        fixUpTable(schema, false);
    }

    public Intent toDiasyncIntent(String source) {
        Intent intent = new Intent();
        intent.setAction("com.eveningoutpost.dexdrip.diasync.libre2_bg");
        intent.setPackage("ru.krotarnya.diasync");

        intent.putExtra("source", source);
        intent.putExtra("libre2_serial", serial);
        intent.putExtra("libre2_value", glucose);
        intent.putExtra("libre2_timestamp", timestamp);

        intent.putExtra("xdrip_sync_key", PreferenceManager.getDefaultSharedPreferences(xdrip.getAppContext()).getString("custom_sync_key", ""));

        Sensor current_sensor = Sensor.currentSensor();
        long start_timestamp = (current_sensor == null) ? timestamp - 1209600000 : current_sensor.started_at;

        List<BgReading> bg_readings = BgReading.latestForGraph(1,  start_timestamp);
        if (bg_readings.size() > 0) {
            BgReading bg_reading = bg_readings.get(0);
            if (bg_reading.calibration != null) {
                intent.putExtra("xdrip_calibration_slope", bg_reading.calibration.slope);
                intent.putExtra("xdrip_calibration_intercept", bg_reading.calibration.intercept);
                intent.putExtra("xdrip_calibration_timestamp", bg_reading.calibration.timestamp);
            }
            else {
                Log.d(TAG, "No associated xdrip_calibration present");
            }
            intent.putExtra("xdrip_value", bg_reading.calculated_value);
            intent.putExtra("xdrip_timestamp", bg_reading.timestamp);
            intent.putExtra("xdrip_arrow", bg_reading.slopeName());
        }
        else {
            Log.d(TAG, "No associated xdrip_value present");
        }
        return intent;
    }
}