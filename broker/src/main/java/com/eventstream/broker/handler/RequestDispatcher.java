package com.eventstream.broker.handler;

import com.eventstream.broker.cluster.ReplicationManager;
import com.eventstream.broker.group.ConsumerGroupManager;
import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Routes a fully-received frame to the appropriate handler.
 * Called exclusively from worker threads (never from the selector thread).
 */
public final class RequestDispatcher {

    private static final Logger log = Logger.getLogger(RequestDispatcher.class.getName());

    private final CreateTopicHandler  createTopicHandler;
    private final ProduceHandler      produceHandler;
    private final FetchHandler        fetchHandler;
    private final JoinGroupHandler    joinGroupHandler;
    private final LeaveGroupHandler   leaveGroupHandler;
    private final HeartbeatHandler    heartbeatHandler;
    private final OffsetCommitHandler offsetCommitHandler;
    private final OffsetFetchHandler  offsetFetchHandler;
    private final ReplicaFetchHandler replicaFetchHandler;
    private final MetadataHandler     metadataHandler;

    public RequestDispatcher(TopicManager topicManager, ConsumerGroupManager groupManager,
                             ReplicationManager replicationManager) {
        this.createTopicHandler  = new CreateTopicHandler(topicManager);
        this.produceHandler      = new ProduceHandler(topicManager, replicationManager);
        this.fetchHandler        = new FetchHandler(topicManager, groupManager, replicationManager);
        this.joinGroupHandler    = new JoinGroupHandler(groupManager);
        this.leaveGroupHandler   = new LeaveGroupHandler(groupManager);
        this.heartbeatHandler    = new HeartbeatHandler(groupManager);
        this.offsetCommitHandler = new OffsetCommitHandler(groupManager);
        this.offsetFetchHandler  = new OffsetFetchHandler(groupManager);
        this.replicaFetchHandler = new ReplicaFetchHandler(topicManager, replicationManager);
        this.metadataHandler     = new MetadataHandler(replicationManager);
    }

    public ByteBuffer dispatch(ByteBuffer frame) {
        if (!frame.hasRemaining()) {
            return ResponseEncoder.errorResponse(ErrorCode.INTERNAL_ERROR);
        }
        byte type = frame.get();
        return switch (type) {
            case RequestType.CREATE_TOPIC  -> createTopicHandler.handle(frame);
            case RequestType.PRODUCE       -> produceHandler.handle(frame);
            case RequestType.FETCH         -> fetchHandler.handle(frame);
            case RequestType.JOIN_GROUP    -> joinGroupHandler.handle(frame);
            case RequestType.LEAVE_GROUP   -> leaveGroupHandler.handle(frame);
            case RequestType.HEARTBEAT     -> heartbeatHandler.handle(frame);
            case RequestType.OFFSET_COMMIT -> offsetCommitHandler.handle(frame);
            case RequestType.OFFSET_FETCH  -> offsetFetchHandler.handle(frame);
            case RequestType.REPLICA_FETCH -> replicaFetchHandler.handle(frame);
            case RequestType.METADATA      -> metadataHandler.handle(frame);
            default -> {
                log.warning("Unknown request type: 0x" + Integer.toHexString(type & 0xFF));
                yield ResponseEncoder.errorResponse(ErrorCode.INTERNAL_ERROR);
            }
        };
    }
}
