package com.eveningoutpost.dexdrip.diasync;

import android.content.Context;
import com.eveningoutpost.dexdrip.models.BloodTest;
import com.eveningoutpost.dexdrip.models.UserError;

public final class DiasyncManualGlucoseExporter {
    private static final String MANUAL_ENTRY_SOURCE = "Manual Entry";
    private static final String TAG = DiasyncManualGlucoseExporter.class.getSimpleName();

    private DiasyncManualGlucoseExporter() {
    }

    public static void export(Context context, BloodTest bloodTest) {
        if (bloodTest == null || !MANUAL_ENTRY_SOURCE.equals(bloodTest.source)) {
            return;
        }
        try {
            if (bloodTest.uuid == null) {
                throw new IllegalArgumentException("Missing blood test UUID");
            }
            DiasyncEventTransport.send(context, new DiasyncManualGlucoseEvent(
                    "MANUAL_GLUCOSE:" + bloodTest.uuid,
                    bloodTest.timestamp,
                    bloodTest.mgdl));
        } catch (IllegalArgumentException exception) {
            UserError.Log.w(TAG, "Diasync manual glucose event was not exported: invalid reading");
        }
    }
}
