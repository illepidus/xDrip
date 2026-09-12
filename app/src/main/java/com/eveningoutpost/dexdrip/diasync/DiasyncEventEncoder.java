package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

final class DiasyncEventEncoder {
    private static final Gson GSON = new Gson();

    String encode(DiasyncEvent event) {
        JsonObject root = new JsonObject();
        root.addProperty("protocolVersion", DiasyncEvent.PROTOCOL_VERSION);
        root.addProperty("eventId", event.eventId());
        root.addProperty("eventType", event.eventType());
        root.addProperty("occurredAtEpochMillis", event.occurredAtEpochMillis());
        event.addPayload(root);
        return GSON.toJson(root);
    }
}
