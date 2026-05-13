package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB segments
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final int id; // Unique partition id
    private int leader; // Leader broker id
    private List<Integer> followers; // Follower Broker Ids for replication
    private final String baseDir; // Directory for log Storage
    private final AtomicLong nextOffset; // Next available message offset
    private final ReadWriteLock lock; // Concurrency control
    private RandomAccessFile activeLogFile; // Current active log File
    private FileChannel activeLogChannel; // Channel for file ops
    private final List<SegmentInfo> segments; // List of segements in partition

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = new ArrayList<>(followers);
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();

        initialize();
    }

    private void initialize() {
        try {
            // Create directory if needed
            File dir = new File(baseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Load existing segements
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String baseName = file.getName().substring(0, file.getName().length() - LOG_SUFFIX.length());
                    long baseOffset = Long.parseLong(baseName);

                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if (indexFile.exists()) {
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(),
                                indexFile.getAbsolutePath());
                        segments.add(segment);
                    }

                }

                // Sort segments by offset
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()));

                // Determine next available offset
                if (!segments.isEmpty()) {
                    SegmentInfo lastSegment = segments.get(segments.size() - 1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment));
                }
            }

            // Create a new segment if none exists
            if (segments.isEmpty()) {
                createNewSegment(0);
            } else {
                // Open last segment as active
                SegmentInfo lastSegment = segments.get(segments.size() - 1);
                openSegmentForAppend(lastSegment);
            }

            LOGGER
                .info("Initialized partition " + id + " with " + segments.size() + " segments, next offset: "
                        + nextOffset.get());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }

    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "replication");
                FileChannel logChannel = logFile.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate(4);

            while (logChannel.position() < logChannel.size()) {
                buf.clear();
                int bytesRead = logChannel.read(buf);
                if (bytesRead < 4)
                    break;

                buf.flip();
                int messageSize = buf.getInt();

                // Skip message
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }
        return count;
    }

    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX;
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX;

        // Create log file
        File logFile = new File(logPath);
        logFile.createNewFile();

        // Create index file
        File indexFile = new File(indexPath);
        indexFile.createNewFile();

        // Add to segments list
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment);

        // Open for append
        openSegmentForAppend(segment);

        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close currently active segment
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }

        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // Open the segment
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();

        // Move to end of file for appending
        activeLogChannel.position(activeLogChannel.size());
    }

    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            long currentOffset = nextOffset.get();

            // Check if we need to roll over to new segment
            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE) {
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }

            // Write message size and data
            ByteBuffer buf = ByteBuffer.allocate(4 + message.length);
            buf.putInt(message.length).put(message).flip();

            // Write to file
            long position = activeLogChannel.position();
            activeLogChannel.write(buf);

            // Force write to disk
            activeLogChannel.force(true);

            // update index
            updateIndex(currentOffset, position);

            // update offset
            nextOffset.incrementAndGet();

            return currentOffset;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void updateIndex(long offset, long position) {
        try {
            if (segments.isEmpty())
                return;

            SegmentInfo currentSegment = segments.get(segments.size() - 1);

            try (RandomAccessFile indexFile = new RandomAccessFile(currentSegment.getIndexPath(), "rw");
                    FileChannel indexChannel = indexFile.getChannel()) {
                indexChannel.position(indexChannel.size());

                ByteBuffer buf = ByteBuffer.allocate(16);
                buf.putLong(offset).putLong(position).flip();

                indexChannel.write(buf);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Faile to update index for partition " + id, e);
        }
    }

    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;

        try {
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null) {
                return messages;
            }

            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0) {
                return messages;
            }

            try (RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                    FileChannel logChannel = logFile.getChannel()) {
                logChannel.position(position);

                ByteBuffer sizeBuf = ByteBuffer.allocate(4);
                long currentOffset = offset;

                while (bytesRead < maxBytes && logChannel.position() < logChannel.size()) {
                    // Read message size
                    sizeBuf.clear();
                    int sizeRead = logChannel.read(sizeBuf);
                    if (sizeRead < 4)
                        break;

                    sizeBuf.flip();
                    int messageSize = sizeBuf.getInt();

                    if (bytesRead + messageSize > maxBytes) {
                        break;
                    }

                    // Read message data
                    ByteBuffer messageBuf = ByteBuffer.allocate(messageSize);
                    int messageRead = logChannel.read(messageBuf);

                    if (messageRead < messageSize) {
                        LOGGER.warning("Incomplete message read at offset " + currentOffset);
                        break;
                    }

                    messageBuf.flip();

                    byte[] message = new byte[messageSize];
                    messageBuf.get(message);
                    messages.add(message);

                    bytesRead += messageSize + 4;
                    currentOffset++;

                    if (logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()) {
                        int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                        if (nextSegmentIndex < segments.size()) {
                            logChannel.close();
                            logFile.close();

                            targetSegment = segments.get(nextSegmentIndex);

                            RandomAccessFile nextLogFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                            FileChannel nextLogChannel = nextLogFile.getChannel();

                            position = 0;
                            nextLogChannel.position(position);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read messages from partition " + id, e);
        } finally {
            lock.readLock().unlock();
        }

        return messages;
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty() || offset >= nextOffset.get()) {
            return null;
        }

        // binary search to find the segment
        int low = 0;
        int high = segments.size() + 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            SegmentInfo segment = segments.get(mid);
            if (mid < segments.size() - 1) {
                SegmentInfo nextSegment = segments.get(mid + 1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()) {
                    return segment;
                }
            } else {
                // Last segment
                if (offset >= segment.getBaseOffset()) {
                    return segment;
                }
            }

            if (offset < segment.getBaseOffset()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath(), "r");
                FileChannel indexChannel = indexFile.getChannel()) {
            if (indexChannel.size() == 0) {
                return 0;
            }

            long relativeOffset = offset - segment.getBaseOffset();

            long entryCount = indexChannel.size() / 16;

            if (relativeOffset >= entryCount) {
                indexChannel.position(indexChannel.size() - 16);
                ByteBuffer buf = ByteBuffer.allocate(16);
                indexChannel.read(buf);
                buf.flip();

                buf.getLong();
                return buf.getLong();
            }

            indexChannel.position(relativeOffset * 16);
            ByteBuffer buf = ByteBuffer.allocate(16);
            indexChannel.read(buf);
            buf.flip();

            buf.getLong();
            return buf.getLong();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to find postion for offset " + offset, e);
            return -1;
        }
    }

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }

    public long getLongEndOffset() {
        return nextOffset.get();
    }

    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null && activeLogChannel.isOpen()) {
                activeLogChannel.close();
            }

            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to close partition resources", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;

        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }

        public long getBaseOffset() {
            return baseOffset;
        }

        public String getLogPath() {
            return logPath;
        }

        public String getIndexPath() {
            return indexPath;
        }
    }
}
