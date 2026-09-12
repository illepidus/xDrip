package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.JsonObject;

final class DiasyncCarbsEvent implements DiasyncEvent {
    private static final String EVENT_TYPE = "CARBS";

    private final String eventId;
    private final long occurredAtEpochMillis;
    private final double grams;
    private final String description;

    DiasyncCarbsEvent(String eventId, long occurredAtEpochMillis, double grams, String description) {
        DiasyncEventValidation.requireEventId(eventId, EVENT_TYPE);
        DiasyncEventValidation.requireTimestamp(occurredAtEpochMillis);
        DiasyncEventValidation.requirePositiveRange(grams, 1_000d, "carbs");
        if (description != null) {
            if (description.isEmpty()
                    || Character.isWhitespace(description.codePointAt(0))
                    || Character.isWhitespace(description.codePointBefore(description.length()))
                    || description.codePointCount(0, description.length()) > 256) {
                throw new IllegalArgumentException("Invalid carbs description");
            }
            for (int index = 0; index < description.length(); index++) {
                if (Character.isISOControl(description.charAt(index))) {
                    throw new IllegalArgumentException("Invalid carbs description");
                }
            }
        }
        this.eventId = eventId;
        this.occurredAtEpochMillis = occurredAtEpochMillis;
        this.grams = grams;
        this.description = description;
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
        JsonObject carbs = new JsonObject();
        carbs.addProperty("grams", grams);
        if (description != null) {
            carbs.addProperty("description", description);
        }
        root.add("carbs", carbs);
    }
}
