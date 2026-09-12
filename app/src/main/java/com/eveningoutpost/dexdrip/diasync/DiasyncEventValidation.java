package com.eveningoutpost.dexdrip.diasync;

final class DiasyncEventValidation {
    private DiasyncEventValidation() {
    }

    static void requireEventId(String value, String eventType) {
        requireIdentifier(value, eventType + ":");
    }

    static void requireIdentifier(String value, String requiredPrefix) {
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

    static void requireTimestamp(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
    }

    static void requirePositiveRange(double value, double maximum, String name) {
        if (!Double.isFinite(value) || value <= 0d || value > maximum) {
            throw new IllegalArgumentException("Invalid " + name);
        }
    }
}
