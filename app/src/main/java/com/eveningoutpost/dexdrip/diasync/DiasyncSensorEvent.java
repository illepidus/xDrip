package com.eveningoutpost.dexdrip.diasync;

final class DiasyncSensorEvent {
    static final int PROTOCOL_VERSION = 1;

    final String eventId;
    final long occurredAtEpochMillis;
    final double rawValue;
    final String sensorId;
    final double calibrationSlope;
    final double calibrationIntercept;

    DiasyncSensorEvent(
            String eventId,
            long occurredAtEpochMillis,
            double rawValue,
            String sensorId,
            double calibrationSlope,
            double calibrationIntercept) {
        requireIdentifier(eventId, "SENSOR:");
        requireIdentifier(sensorId, null);
        requireRange(rawValue, 0d, 1_000_000d, "raw value");
        requireRange(calibrationSlope, 0d, 1_000d, "calibration slope");
        if (!Double.isFinite(calibrationIntercept)
                || Math.abs(calibrationIntercept) > 1_000_000d) {
            throw new IllegalArgumentException("Invalid calibration intercept");
        }
        if (occurredAtEpochMillis < 0) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
        this.eventId = eventId;
        this.occurredAtEpochMillis = occurredAtEpochMillis;
        this.rawValue = rawValue;
        this.sensorId = sensorId;
        this.calibrationSlope = calibrationSlope;
        this.calibrationIntercept = calibrationIntercept;
    }

    private static void requireIdentifier(String value, String requiredPrefix) {
        if (value == null
                || value.isEmpty()
                || value.length() > 128
                || !value.equals(value.trim())
                || (requiredPrefix != null
                        && (value.length() == requiredPrefix.length()
                                || !value.startsWith(requiredPrefix)))) {
            throw new IllegalArgumentException("Invalid identifier");
        }
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character < 0x21 || character > 0x7e) {
                throw new IllegalArgumentException("Invalid identifier");
            }
        }
    }

    private static void requireRange(double value, double lowerExclusive, double upperInclusive, String name) {
        if (!Double.isFinite(value) || value <= lowerExclusive || value > upperInclusive) {
            throw new IllegalArgumentException("Invalid " + name);
        }
    }
}
