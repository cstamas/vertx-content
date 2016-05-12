package org.cstamas.vertx.content.impl;

import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.ContentManager;
import org.cstamas.vertx.content.FlowControl;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.ContentManager.require;
import static org.cstamas.vertx.content.ContentManager.txId;

/**
 * {@link ContentManager} implementation that establishes {@link FlowControl} and passes the work to {@link Transport}.
 */
public class ContentManagerImpl
    implements ContentManager
{
  protected static final Logger log = LoggerFactory.getLogger(ContentManagerImpl.class);

  private static final String FLOW_ADDRESS_PREFIX = "contentManager.flow.";

  private final Vertx vertx;

  // TODO: a map of transports?
  private final Transport transport;

  public ContentManagerImpl(final Vertx vertx, final Transport transport) {
    this.vertx = checkNotNull(vertx);
    this.transport = checkNotNull(transport);
  }

  @Override
  public ContentManagerImpl send(final ReadStream<Buffer> stream, final Handler<AsyncResult<JsonObject>> handler)
  {
    checkNotNull(stream);
    vertx.getOrCreateContext().runOnContext(
        w -> {
          Future<JsonObject> future = Future.future();
          try {
            final String txId = UUID.randomUUID().toString();
            final String senderFlowAddress = FLOW_ADDRESS_PREFIX + txId + ".s";
            final String receiverFlowAddress = FLOW_ADDRESS_PREFIX + txId + ".r";
            JsonObject contentHandle = new JsonObject()
                .put(TXID, txId)
                .put("transport", transport.name())
                .put("senderFlowAddress", senderFlowAddress)
                .put("receiverFlowAddress", receiverFlowAddress);
            final FlowControl flowControl = new FlowControl(vertx, senderFlowAddress, receiverFlowAddress);
            transport.send(contentHandle, flowControl, stream);
            future.complete(contentHandle);
          }
          catch (Exception e) {
            future.fail(e);
          }
          if (handler != null) {
            handler.handle(future);
          }
        }
    );
    return this;
  }

  @Override
  public ContentManagerImpl receive(final JsonObject contentHandle,
                                    final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler)
  {
    checkNotNull(contentHandle);
    txId(contentHandle); // SANITY
    checkArgument(transport.name().equals(contentHandle.getString("transport")), "Invalid transport: %s"); // SANITY
    vertx.getOrCreateContext().runOnContext(
        w -> {
          Future<ReadStream<Buffer>> future = Future.future();
          try {
            future.complete(
                transport.receive(
                    contentHandle,
                    new FlowControl(vertx,
                        require(contentHandle, "receiverFlowAddress"),
                        require(contentHandle, "senderFlowAddress")
                    )
                )
            );
          }
          catch (Exception e) {
            future.fail(e);
          }
          if (streamHandler != null) {
            streamHandler.handle(future);
          }
        }
    );
    return this;
  }
}
