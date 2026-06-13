package io.plurima.kafka.deserializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordDeserializerTest {

    @Test
    void bytesReturnsCachedIdentitySingleton() {
        // The fast path in ClassicPollLoop and ListenerInvoker uses reference equality
        // (deser == RecordDeserializer.IDENTITY_BYTES) to detect the bytes-in/bytes-out
        // case. If RecordDeserializer.bytes() returned a fresh lambda each call, the
        // check would never trigger and the per-record ConsumerRecord allocation would
        // not be elided. Lock this in.
        RecordDeserializer<byte[]> a = RecordDeserializer.bytes();
        RecordDeserializer<byte[]> b = RecordDeserializer.bytes();
        assertThat(a)
            .as("bytes() must return the same cached IDENTITY_BYTES instance every call")
            .isSameAs(b)
            .isSameAs(RecordDeserializer.IDENTITY_BYTES);
    }

    @Test
    void identityDeserializerReturnsInputUnchanged() {
        byte[] input = "hello".getBytes();
        byte[] result = RecordDeserializer.IDENTITY_BYTES.deserialize("any-topic", input);
        assertThat(result)
            .as("IDENTITY_BYTES must return the input byte[] reference unchanged (no copy)")
            .isSameAs(input);
    }

    @Test
    void identityDeserializerPassesThroughNullBytes() {
        byte[] result = RecordDeserializer.IDENTITY_BYTES.deserialize("t", null);
        assertThat(result).isNull();
    }

    @Test
    void utf8DeserializerDecodesBytes() {
        RecordDeserializer<String> deser = RecordDeserializer.utf8String();
        assertThat(deser.deserialize("t", "café".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
            .isEqualTo("café");
        assertThat(deser.deserialize("t", null)).isNull();
        assertThat(deser.deserialize("t", new byte[0])).isEmpty();
    }

    @Test
    void utf8DeserializerIsNotConfusedWithIdentitySingleton() {
        // Important to confirm: the fast path must NOT trigger for utf8String — the
        // user expects String output, not byte[]. utf8String() returns its own lambda
        // (no caching needed since the type is different), distinct from IDENTITY_BYTES.
        RecordDeserializer<String> utf8 = RecordDeserializer.utf8String();
        assertThat((Object) utf8)
            .as("utf8String must not collide with IDENTITY_BYTES singleton")
            .isNotSameAs(RecordDeserializer.IDENTITY_BYTES);
    }
}
