/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TrackDelayedDeliveryClockSkewTest {

    private PersistentTopic topicMock;

    @BeforeMethod
    public void setup() {
        topicMock = mock(PersistentTopic.class);
        doReturn(true).when(topicMock).isDelayedDeliveryEnabled();
        doCallRealMethod().when(topicMock).correctDeliverAtTimeForClockSkew(ArgumentMatchers.any(ByteBuf.class));
    }

    private ByteBuf createMessageBuf(MessageMetadata metadata) {
        ByteBuf payload = Unpooled.wrappedBuffer("test-payload".getBytes());
        return Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, metadata, payload);
    }

    private MessageMetadata parseMetadata(ByteBuf buf) {
        buf.markReaderIndex();
        MessageMetadata md = new MessageMetadata();
        Commands.parseMessageMetadata(buf, md);
        buf.resetReaderIndex();
        return md;
    }

    /**
     * Client clock is 5 minutes behind broker.
     * Client uses deliverAfter(3min):
     *   publishTime = clientNow = brokerNow - 5min
     *   deliverAtTime = clientNow + 3min = brokerNow - 2min
     *
     * Without fix: broker persists deliverAtTime = brokerNow - 2min, already past -> delivers immediately.
     * With fix: broker corrects to brokerNow + 3min at publish time.
     */
    @Test
    public void testClientClockBehindServer() {
        long brokerNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = brokerNow - clockSkew;
        long delayMs = 3 * 60 * 1000L;

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(clientNow);
        metadata.setDeliverAtTime(clientNow + delayMs);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf corrected = topicMock.correctDeliverAtTimeForClockSkew(original);

        MessageMetadata correctedMd = parseMetadata(corrected);
        long correctedDeliverAt = correctedMd.getDeliverAtTime();

        // Should be approximately brokerNow + 3min
        assertTrue(Math.abs(correctedDeliverAt - (brokerNow + delayMs)) < 1000,
                String.format("Expected ~%d, got %d (diff: %d ms)",
                        brokerNow + delayMs, correctedDeliverAt,
                        correctedDeliverAt - (brokerNow + delayMs)));

        corrected.release();
    }

    /**
     * Client clock is 5 minutes ahead of broker.
     * Client uses deliverAfter(3min):
     *   publishTime = clientNow = brokerNow + 5min
     *   deliverAtTime = clientNow + 3min = brokerNow + 8min
     *
     * Without fix: broker persists deliverAtTime = brokerNow + 8min, delivers 5 minutes too late.
     * With fix: broker corrects to brokerNow + 3min at publish time.
     */
    @Test
    public void testClientClockAheadOfServer() {
        long brokerNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = brokerNow + clockSkew;
        long delayMs = 3 * 60 * 1000L;

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(clientNow);
        metadata.setDeliverAtTime(clientNow + delayMs);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf corrected = topicMock.correctDeliverAtTimeForClockSkew(original);

        MessageMetadata correctedMd = parseMetadata(corrected);
        long correctedDeliverAt = correctedMd.getDeliverAtTime();

        assertTrue(Math.abs(correctedDeliverAt - (brokerNow + delayMs)) < 1000,
                String.format("Expected ~%d, got %d (diff: %d ms)",
                        brokerNow + delayMs, correctedDeliverAt,
                        correctedDeliverAt - (brokerNow + delayMs)));

        corrected.release();
    }

    /**
     * No clock skew: corrected value should match the original.
     */
    @Test
    public void testNoClockSkew() {
        long brokerNow = System.currentTimeMillis();
        long delayMs = 60_000;

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(brokerNow);
        metadata.setDeliverAtTime(brokerNow + delayMs);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf corrected = topicMock.correctDeliverAtTimeForClockSkew(original);

        MessageMetadata correctedMd = parseMetadata(corrected);
        long correctedDeliverAt = correctedMd.getDeliverAtTime();

        assertTrue(Math.abs(correctedDeliverAt - (brokerNow + delayMs)) < 1000,
                "With no clock skew, corrected time should match original");

        corrected.release();
    }

    /**
     * No deliverAtTime but publishTime is set: should return ByteBuf unchanged.
     * (publishTime is a required field in MessageMetadata, so it's always present.)
     */

    /**
     * No deliverAtTime: should return ByteBuf unchanged.
     */
    @Test
    public void testNoDeliverAtTime() {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(System.currentTimeMillis());
        // No deliverAtTime set

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf result = topicMock.correctDeliverAtTimeForClockSkew(original);

        assertTrue(result == original, "Should return same ByteBuf when deliverAtTime is missing");

        result.release();
    }

    /**
     * relativeDelay <= 0 (deliverAtTime <= publishTime): should return ByteBuf unchanged.
     */
    @Test
    public void testNegativeRelativeDelay() {
        long now = System.currentTimeMillis();

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(now);
        metadata.setDeliverAtTime(now - 1000); // deliverAtTime before publishTime

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf result = topicMock.correctDeliverAtTimeForClockSkew(original);

        assertTrue(result == original, "Should return same ByteBuf when relativeDelay <= 0");

        MessageMetadata resultMd = parseMetadata(result);
        assertEquals(resultMd.getDeliverAtTime(), now - 1000);

        result.release();
    }

    /**
     * Delayed delivery is disabled: should return ByteBuf unchanged.
     */
    @Test
    public void testDelayedDeliveryDisabled() {
        doReturn(false).when(topicMock).isDelayedDeliveryEnabled();

        long brokerNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = brokerNow - clockSkew;

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(clientNow);
        metadata.setDeliverAtTime(clientNow + 60_000);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf result = topicMock.correctDeliverAtTimeForClockSkew(original);

        assertTrue(result == original, "Should return same ByteBuf when delayed delivery is disabled");

        result.release();
    }

    /**
     * Verify that non-deliverAtTime fields in metadata are preserved after correction.
     */
    @Test
    public void testMetadataFieldsPreserved() {
        long brokerNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = brokerNow - clockSkew;
        long delayMs = 3 * 60 * 1000L;

        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test-producer");
        metadata.setSequenceId(42);
        metadata.setPublishTime(clientNow);
        metadata.setDeliverAtTime(clientNow + delayMs);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf corrected = topicMock.correctDeliverAtTimeForClockSkew(original);

        MessageMetadata correctedMd = parseMetadata(corrected);

        // Verify other fields are preserved
        assertEquals(correctedMd.getProducerName(), "test-producer");
        assertEquals(correctedMd.getSequenceId(), 42);
        assertEquals(correctedMd.getPublishTime(), clientNow);

        corrected.release();
    }

    /**
     * Demonstrate that publish-time correction is immune to backlog.
     *
     * Scenario: client clock is 5 minutes behind, deliverAfter(3min).
     * The message is corrected at publish time to deliverAt = brokerPublishTime + 3min.
     * Then it sits in backlog for 2 minutes before the dispatcher reads it.
     *
     * publish-time approach:
     *   deliverAt was already corrected and persisted as brokerPublishTime + 3min at publish time.
     *   At dispatch time (2 min later), the remaining delay is only 1 min, which is correct.
     */
    @Test
    public void testBacklogDoesNotInflateDelay() {
        long brokerPublishTime = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = brokerPublishTime - clockSkew;
        long delayMs = 3 * 60 * 1000L;
        long backlogDuration = 2 * 60 * 1000L;

        // Step 1: at publish time, broker corrects deliverAtTime
        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test");
        metadata.setSequenceId(1);
        metadata.setPublishTime(clientNow);
        metadata.setDeliverAtTime(clientNow + delayMs);

        ByteBuf original = createMessageBuf(metadata);
        ByteBuf corrected = topicMock.correctDeliverAtTimeForClockSkew(original);

        MessageMetadata correctedMd = parseMetadata(corrected);
        long persistedDeliverAt = correctedMd.getDeliverAtTime();

        // The persisted deliverAtTime should be ~ brokerPublishTime + 3min
        long expectedDeliverAt = brokerPublishTime + delayMs;
        assertTrue(Math.abs(persistedDeliverAt - expectedDeliverAt) < 1000,
                String.format("Persisted deliverAt should be ~%d, got %d", expectedDeliverAt, persistedDeliverAt));

        // Step 2: simulate backlog — dispatcher reads entry 2 minutes later
        long dispatchTime = brokerPublishTime + backlogDuration;

        // The remaining delay at dispatch time should be ~ 1 minute, NOT 3 minutes
        long remainingDelay = persistedDeliverAt - dispatchTime;
        long expectedRemaining = delayMs - backlogDuration; // 3min - 2min = 1min

        assertTrue(Math.abs(remainingDelay - expectedRemaining) < 1000,
                String.format("Remaining delay at dispatch should be ~%d ms (1 min), got %d ms. "
                        + "If this were ~%d ms (3 min), the old dispatch-time bug is present.",
                        expectedRemaining, remainingDelay, delayMs));

        // Step 3: contrast with the old buggy dispatch-time approach
        long buggyDeliverAt = dispatchTime + delayMs;
        long buggyRemaining = buggyDeliverAt - dispatchTime; // always = delayMs = 3min

        assertEquals(buggyRemaining, delayMs,
                "Buggy approach always restarts full delay regardless of backlog");
        assertTrue(buggyDeliverAt - persistedDeliverAt >= backlogDuration - 1000,
                String.format("Buggy approach over-delays by ~backlog duration (%d ms)", backlogDuration));

        corrected.release();
    }
}
