package com.eveningoutpost.dexdrip.diasync;

import android.content.Context;
import com.eveningoutpost.dexdrip.models.Treatments;
import com.eveningoutpost.dexdrip.models.UserError;

public final class DiasyncCarbsExporter {
    private static final int MAX_DESCRIPTION_CODE_POINTS = 256;
    private static final String TAG = DiasyncCarbsExporter.class.getSimpleName();

    private DiasyncCarbsExporter() {
    }

    public static void export(Context context, Treatments treatment) {
        if (treatment == null || treatment.carbs <= 0d) {
            return;
        }
        try {
            if (treatment.uuid == null) {
                throw new IllegalArgumentException("Missing treatment UUID");
            }
            DiasyncEventTransport.send(context, new DiasyncCarbsEvent(
                    "CARBS:" + treatment.uuid,
                    treatment.timestamp,
                    treatment.carbs,
                    descriptionFrom(treatment.notes)));
        } catch (IllegalArgumentException exception) {
            UserError.Log.w(TAG, "Diasync carbs event was not exported: invalid treatment");
        }
    }

    static String descriptionFrom(String notes) {
        if (notes == null) {
            return null;
        }
        String stripped = stripWhitespace(notes);
        if (stripped.isEmpty()) {
            return null;
        }
        StringBuilder sanitized = new StringBuilder(stripped.length());
        for (int index = 0; index < stripped.length();) {
            int codePoint = stripped.codePointAt(index);
            sanitized.appendCodePoint(Character.isISOControl(codePoint) ? ' ' : codePoint);
            index += Character.charCount(codePoint);
        }
        String description = stripWhitespace(sanitized.toString());
        if (description.isEmpty()) {
            return null;
        }
        int count = description.codePointCount(0, description.length());
        if (count > MAX_DESCRIPTION_CODE_POINTS) {
            description = description.substring(
                    0,
                    description.offsetByCodePoints(0, MAX_DESCRIPTION_CODE_POINTS));
            description = stripWhitespace(description);
        }
        return description.isEmpty() ? null : description;
    }

    private static String stripWhitespace(String value) {
        int start = 0;
        while (start < value.length()) {
            int codePoint = value.codePointAt(start);
            if (!Character.isWhitespace(codePoint)) {
                break;
            }
            start += Character.charCount(codePoint);
        }
        int end = value.length();
        while (end > start) {
            int codePoint = value.codePointBefore(end);
            if (!Character.isWhitespace(codePoint)) {
                break;
            }
            end -= Character.charCount(codePoint);
        }
        return value.substring(start, end);
    }
}
