package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.util.List;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Broker internal
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIF = 0x23;


    // Send Error Response to Client
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(3 + errorMessage.length());
        buf
            .put(ERROR_RESPONSE)
            .putShort((short) errorMessage.length())
            .put(errorMessage.getBytes())
            .flip();
            
        channel.write(buf);
    }

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
    
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        ByteBuffer buf = ByteBuffer.allocate(17 + topic.length() + message.length);
        buf
            .put(REPLICATE)
            .putShort((short) topic.length())
            .put(topic.getBytes())
            .putInt(partition)
            .putLong(offset)
            .putInt(message.length)
            .put(message)
            .flip();
            
        return buf;
    }
    
    public static ByteBuffer encodeTopicNotification(String topic) {
        ByteBuffer buf = ByteBuffer.allocate(3 + topic.length());
        buf
            .put(TOPIC_NOTIF)
            .putShort((short) topic.length())
            .put(topic.getBytes())
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
    
    public static FetchResult decodeFetchResponse(ByteBuffer buf) {
        byte responseType = buf.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buf.getShort();
                byte[] errorBytes = new byte[errorLength];
                buf.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }
        
        int messageCount = buf.getInt();
        byte[][] messages = new byte[messageCount][];
        
        for (int i = 0; i < messageCount; i++) {
            long offset = buf.getLong();
            int messageSize = buf.getInt();
            messages[i] = new byte[messageSize];
            buf.get(messages[i]);
        }
        
        return new FetchResult(messages, null);
    }
    
    public static MetadataResult decodeMetadataResult(ByteBuffer buf) {
        byte responseType = buf.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buf.getShort();
                byte[] errorBytes = new byte[errorLength];
                buf.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }
        
        // parse broker info
        int brokerCount = buf.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();
        
        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buf.getInt();
            short hostLength = buf.getShort();
            byte[] hostBytes = new byte[hostLength];
            buf.get(hostBytes);
            String host = new String(hostBytes);
            int port = buf.getInt();
            
            brokers.add(new BrokerInfo(brokerId, host, port));
        }
        
        // parse topic metadata
        int topicCount = buf.getInt();
        List<TopicMetadata> topics = new ArrayList<>();
        
        for (int i = 0; i < topicCount; i++) {
            short topicLength = buf.getShort();
            byte[] topicBytes = new byte[topicLength];
            buf.get(topicBytes);
            String topicName = new String(topicBytes);
            
            int partitionCount = buf.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();
            
            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buf.getInt();
                int leaderId = buf.getInt();
                
                int replicas = buf.getInt();
                List<Integer> replicaIds = new ArrayList<>();
                
                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buf.getInt());
                }
                
                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }
            
            topics.add(new TopicMetadata(topicName, partitions));
        }
        
        return new MetadataResult(brokers, topics, null);
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

    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }
    
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;
        
        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }
        
        public String getName() {
            return name;
        }
        
        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }
    
    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;
        
        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = id;
            this.replicas = replicas;
        }
        
        public int getId() {
            return id;
        }
        
        public int getLeader() {
            return leader;
        }
        
        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}
