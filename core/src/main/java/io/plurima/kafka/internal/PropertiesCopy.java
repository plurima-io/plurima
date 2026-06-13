package io.plurima.kafka.internal;

import io.plurima.kafka.annotation.Internal;

import java.util.Properties;

/**
 * Defensive copy of a {@link Properties} that preserves entries backed by a
 * defaults table.
 *
 * <p>{@code new Properties().putAll(src)} silently drops entries that live only
 * in {@code src}'s {@code defaults} reference (the {@code Properties(Properties
 * defaults)} constructor pattern is common in older Kafka tooling and Spring
 * Environment-backed property sources). The defaults are reachable through
 * {@link Properties#getProperty(String)} but invisible to {@code Hashtable.putAll}.
 *
 * <p>Using {@link Properties#stringPropertyNames()} walks both the explicit
 * entries and the defaults chain, so {@code copy(src)} preserves every key a
 * caller could read via {@code src.getProperty(key)}.
 *
 * <p>This helper restricts itself to string keys + string values — the
 * {@link Properties} contract documents that as the intended use, and Kafka
 * client configs never deviate.
 */
@Internal
public final class PropertiesCopy {

    private PropertiesCopy() {}

    /**
     * Deep-copy {@code src} into a new {@link Properties}, preserving:
     *
     * <ul>
     *   <li>Explicit entries (including non-String values like
     *       {@code Integer} — Kafka client configs accept boxed numerics).</li>
     *   <li>Values reachable only through the {@code defaults} chain that
     *       backs {@code src} — the case {@code new Properties().putAll(src)}
     *       silently drops because {@code Hashtable.putAll} walks only the
     *       immediate entries.</li>
     * </ul>
     *
     * <p>Returns an empty {@link Properties} when {@code src} is {@code null}.
     */
    public static Properties copy(Properties src) {
        Properties out = new Properties();
        if (src == null) return out;
        // Step 1: copy all explicit hashtable entries — preserves non-String values.
        out.putAll(src);
        // Step 2: walk the defaults chain via stringPropertyNames() and fill in any
        // string-typed keys that weren't in the explicit hashtable. setProperty is
        // a no-op when the key already exists with the same value.
        for (String key : src.stringPropertyNames()) {
            if (!out.containsKey(key)) {
                String value = src.getProperty(key);
                if (value != null) {
                    out.setProperty(key, value);
                }
            }
        }
        return out;
    }
}
