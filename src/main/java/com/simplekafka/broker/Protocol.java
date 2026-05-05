package com.simplekafka.broker;

import java.nio.ByteBuffer;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATE_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Broker internal
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIF = 0x23;

    // Encode Request Methods
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        /**
         * Request Type (1) + topic string len (2) + Topic string (n) + Partition (4) +
         * message len (4) + message (n)
         */
        ByteBuffer buf = ByteBuffer.allocate(11 + topic.length() + message.length);
        buf
            .put(PRODUCE)
            .putShort((short) topic.length())
            .put(topic.getBytes())
            .putInt(partition)
            .putInt(message.length)
            .put(message);
        return buf;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        /**
         * Request Type (1) + topic string len (2) + Topic string (n) + Partition (4) +
         * offset (8) + maxBytes (4)
         */
        ByteBuffer buf = ByteBuffer.allocate(19 + topic.length());
        buf
            .put(FETCH)
            .putShort((short) topic.length())
            .put(topic.getBytes())
            .putInt(partition)
            .putLong(offset)
            .putInt(maxBytes)
            .flip();
        return buf;
    }

    public static ByteBuffer encodeMetadataRequest() {
        // Just request type (1)
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put(METADATA).flip();
        return buf;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        /**
         * Request Type (1) + topic string len (2) + Topic string (n) + numPartition (4)
         * + replicationFactor(2)
         */
        ByteBuffer buf = ByteBuffer.allocate(9 + topic.length());
        buf
            .put(CREATE_TOPIC)
            .putShort((short) topic.length())
            .put(topic.getBytes())
            .putInt(numPartitions)
            .putShort(replicationFactor)
            .flip();
        return buf;
    }

    // Decode Response Methods
    public static ProduceResult decodeProduceResponse(ByteBuffer buf) {
        byte responseType = buf.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buf.getShort();
                byte[] errorBytes = new byte[errorLength];
                buf.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buf.getLong();
        byte status = buf.get();

        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

    // Response Classes
    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }
}
