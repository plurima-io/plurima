/**
 * Explicit-acknowledgement types for the SHARE engine: {@link io.plurima.kafka.ack.AckContext}
 * and {@link io.plurima.kafka.ack.AckMessage} let a
 * {@link io.plurima.kafka.ack.ManualAckListener} or
 * {@link io.plurima.kafka.ack.MessageAckListener} decide a record's disposition
 * (accept/release/reject) itself instead of relying on the implicit accept-on-normal-return
 * behavior of {@link io.plurima.kafka.RecordListener} / {@link io.plurima.kafka.MessageListener}.
 */
@NullMarked
package io.plurima.kafka.ack;

import org.jspecify.annotations.NullMarked;
