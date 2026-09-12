package com.eveningoutpost.dexdrip.diasync;

import com.google.gson.JsonObject;

interface DiasyncEvent {
    int PROTOCOL_VERSION = 1;

    String eventId();

    String eventType();

    long occurredAtEpochMillis();

    void addPayload(JsonObject root);
}
