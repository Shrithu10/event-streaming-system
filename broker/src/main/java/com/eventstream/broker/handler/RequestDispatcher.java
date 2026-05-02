package com.eventstream.broker.handler;

import com.eventstream.broker.network.ResponseEncoder;
import com.eventstream.broker.topic.TopicManager;
import com.eventstream.common.protocol.ErrorCode;
import com.eventstream.common.protocol.RequestType;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * Reads the request-type byte from a fully-received frame and dispatches to
 * the appropriate handler.  All state lives in the individual handlers.
 */
public final class RequestDispatcher {

    private static final Logger log = Logger.getLogger(RequestDispatcher.class.getName());

    private final CreateTopicHandler createTopicHandler;
    private final ProduceHandler     produceHandler;
    private final FetchHandler       fetchHandler;

    public RequestDispatcher(TopicManager topicManager) {
        this.createTopicHandler = new CreateTopicHandler(topicManager);
        this.produceHandler     = new ProduceHandler(topicManager);
        this.fetchHandler       = new FetchHandler(topicManager);
    }

    /**
     * @param frame complete frame body (type byte + payload), ready to read
     * @return response ByteBuffer (flipped, ready to write to channel)
     */
    public ByteBuffer dispatch(ByteBuffer frame) {
        if (!frame.hasRemaining()) {
            return ResponseEncoder.errorResponse(ErrorCode.INTERNAL_ERROR);
        }
        byte type = frame.get();
        return switch (type) {
            case RequestType.CREATE_TOPIC -> createTopicHandler.handle(frame);
            case RequestType.PRODUCE      -> produceHandler.handle(frame);
            case RequestType.FETCH        -> fetchHandler.handle(frame);
            default -> {
                log.warning("Unknown request type: 0x" + Integer.toHexString(type & 0xFF));
                yield ResponseEncoder.errorResponse(ErrorCode.INTERNAL_ERROR);
            }
        };
    }
}
