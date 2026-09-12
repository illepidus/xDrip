package com.eveningoutpost.dexdrip.diasync;

import android.content.Context;
import android.content.Intent;

final class DiasyncEventTransport {
    static final String ACTION_XDRIP_EVENT = "ru.krotarnya.diasync2.action.XDRIP_EVENT";
    static final String DIASYNC_PACKAGE = "ru.krotarnya.diasync2";
    static final String EXTRA_PAYLOAD = "payload";

    private static final DiasyncEventEncoder ENCODER = new DiasyncEventEncoder();

    private DiasyncEventTransport() {
    }

    static void send(Context context, DiasyncEvent event) {
        Intent intent = new Intent(ACTION_XDRIP_EVENT)
                .setPackage(DIASYNC_PACKAGE)
                .putExtra(EXTRA_PAYLOAD, ENCODER.encode(event));
        context.sendBroadcast(intent);
    }
}
