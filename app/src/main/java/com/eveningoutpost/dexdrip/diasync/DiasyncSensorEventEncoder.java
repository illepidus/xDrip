package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

final class DiasyncSensorEventEncoder {
    private static final Gson GSON = new Gson();

    String encode(DiasyncSensorEvent event) {
        JsonObject root = new JsonObject();
        root.addProperty("protocolVersion", DiasyncSensorEvent.PROTOCOL_VERSION);
        root.addProperty("eventId", event.eventId);
        root.addProperty("eventType", "SENSOR");
        root.addProperty("occurredAtEpochMillis", event.occurredAtEpochMillis);

        JsonObject sensor = new JsonObject();
        sensor.addProperty("rawValue", event.rawValue);
        sensor.addProperty("sensorId", event.sensorId);
        JsonObject calibration = new JsonObject();
        calibration.addProperty("slope", event.calibrationSlope);
        calibration.addProperty("intercept", event.calibrationIntercept);
        sensor.add("calibration", calibration);
        root.add("sensor", sensor);
        return GSON.toJson(root);
    }
}
