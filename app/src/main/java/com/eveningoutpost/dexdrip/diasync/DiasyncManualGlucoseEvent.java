package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.JsonObject;

final class DiasyncManualGlucoseEvent implements DiasyncEvent {
    private static final String EVENT_TYPE = "MANUAL_GLUCOSE";

    private final String eventId;
    private final long occurredAtEpochMillis;
    private final double mgdl;

    DiasyncManualGlucoseEvent(String eventId, long occurredAtEpochMillis, double mgdl) {
        DiasyncEventValidation.requireEventId(eventId, EVENT_TYPE);
        DiasyncEventValidation.requireTimestamp(occurredAtEpochMillis);
        DiasyncEventValidation.requirePositiveRange(mgdl, 1_000d, "manual glucose");
        this.eventId = eventId;
        this.occurredAtEpochMillis = occurredAtEpochMillis;
        this.mgdl = mgdl;
    }

    @Override
    public String eventId() {
        return eventId;
    }

    @Override
    public String eventType() {
        return EVENT_TYPE;
    }

    @Override
    public long occurredAtEpochMillis() {
        return occurredAtEpochMillis;
    }

    @Override
    public void addPayload(JsonObject root) {
        JsonObject manualGlucose = new JsonObject();
        manualGlucose.addProperty("mgdl", mgdl);
        root.add("manualGlucose", manualGlucose);
    }
}
