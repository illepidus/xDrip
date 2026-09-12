package com.eveningoutpost.dexdrip.diasync;

import android.content.Context;
import com.eveningoutpost.dexdrip.models.Calibration;
import com.eveningoutpost.dexdrip.models.Libre2RawValue;
import com.eveningoutpost.dexdrip.models.UserError;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class DiasyncSensorExporter {
    private static final String TAG = DiasyncSensorExporter.class.getSimpleName();
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    public static final String ACTION_XDRIP_EVENT = DiasyncEventTransport.ACTION_XDRIP_EVENT;
    public static final String DIASYNC_PACKAGE = DiasyncEventTransport.DIASYNC_PACKAGE;
    public static final String EXTRA_PAYLOAD = DiasyncEventTransport.EXTRA_PAYLOAD;

    private DiasyncSensorExporter() {
    }

    public static void export(Context context, Libre2RawValue rawValue, Calibration calibration) {
        try {
            DiasyncSensorEvent event = eventFrom(rawValue, calibration);
            DiasyncEventTransport.send(context, event);
        } catch (IllegalArgumentException exception) {
            UserError.Log.w(TAG, "Diasync sensor event was not exported: invalid reading");
        }
    }

    static DiasyncSensorEvent eventFrom(Libre2RawValue rawValue, Calibration calibration) {
        if (rawValue == null) {
            throw new IllegalArgumentException("Missing raw value");
        }
        double slope = calibration == null ? 1d : calibration.slope;
        double intercept = calibration == null ? 0d : calibration.intercept;
        return new DiasyncSensorEvent(
                eventId(rawValue.serial, rawValue.timestamp),
                rawValue.timestamp,
                rawValue.glucose,
                rawValue.serial,
                slope,
                intercept);
    }

    private static String eventId(String sensorId, long timestamp) {
        if (sensorId == null) {
            throw new IllegalArgumentException("Missing sensor id");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(sensorId.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            byte[] hash = digest.digest(Long.toString(timestamp).getBytes(StandardCharsets.US_ASCII));
            StringBuilder encoded = new StringBuilder("SENSOR:");
            for (byte value : hash) {
                encoded.append(HEX[(value >>> 4) & 0x0f]);
                encoded.append(HEX[value & 0x0f]);
            }
            return encoded.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }
}
