package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.JsonObject;

final class DiasyncSensorEvent implements DiasyncEvent {

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
        DiasyncEventValidation.requireEventId(eventId, "SENSOR");
        DiasyncEventValidation.requireIdentifier(sensorId, null);
        DiasyncEventValidation.requirePositiveRange(rawValue, 1_000_000d, "raw value");
        DiasyncEventValidation.requirePositiveRange(calibrationSlope, 1_000d, "calibration slope");
        if (!Double.isFinite(calibrationIntercept)
                || Math.abs(calibrationIntercept) > 1_000_000d) {
            throw new IllegalArgumentException("Invalid calibration intercept");
        }
        DiasyncEventValidation.requireTimestamp(occurredAtEpochMillis);
        this.eventId = eventId;
        this.occurredAtEpochMillis = occurredAtEpochMillis;
        this.rawValue = rawValue;
        this.sensorId = sensorId;
        this.calibrationSlope = calibrationSlope;
        this.calibrationIntercept = calibrationIntercept;
    }

    @Override
    public String eventId() {
        return eventId;
    }

    @Override
    public String eventType() {
        return "SENSOR";
    }

    @Override
    public long occurredAtEpochMillis() {
        return occurredAtEpochMillis;
    }

    @Override
    public void addPayload(JsonObject root) {
        JsonObject sensor = new JsonObject();
        sensor.addProperty("rawValue", rawValue);
        sensor.addProperty("sensorId", sensorId);
        JsonObject calibration = new JsonObject();
        calibration.addProperty("slope", calibrationSlope);
        calibration.addProperty("intercept", calibrationIntercept);
        sensor.add("calibration", calibration);
        root.add("sensor", sensor);
    }
}
