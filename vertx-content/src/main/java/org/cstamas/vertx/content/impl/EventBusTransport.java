package org.cstamas.vertx.content.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.impl.HandlerRegistration;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.FlowControl;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.impl.ContentManagerImpl.require;
import static org.cstamas.vertx.content.impl.ContentManagerImpl.txId;

/**
 * {@link Transport} implementation that uses {@link EventBus} for transport.
 */
public class EventBusTransport
    implements Transport
{
  private static final Logger log = LoggerFactory.getLogger(EventBusTransport.class);

  private final Vertx vertx;

  public EventBusTransport(final Vertx vertx) {
    this.vertx = checkNotNull(vertx);
    log.info("Created " + getClass().getSimpleName());
  }

  @Override
  public void send(final JsonObject contentHandle,
                   final FlowControl flowControl,
                   final ReadStream<Buffer> stream)
  {
    String txId = txId(contentHandle);
    String contentAddress = "contentManagerEb." + txId;
    MessageProducer<Buffer> contentSender = vertx.eventBus().sender(contentAddress);
    Pump pump = Pump.pump(stream, contentSender);

    contentHandle.put("contentAddress", contentAddress);

    // stream tells when we end flow
    stream.endHandler(
        v -> {
          flowControl.end();
        }
    );

    // receiver controls us
    flowControl.setOnBeginHandler(
        j -> {
          log.info("BEGIN: " + txId);
          pump.start();
        }
    );
  }

  @Override
  public void receive(final JsonObject contentHandle,
                      final FlowControl flowControl,
                      final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler)
  {
    Future<ReadStream<Buffer>> future = Future.future();
    try {
      String txId = txId(contentHandle);
      String contentAddress = require(contentHandle, "contentAddress");
      MessageConsumer<Buffer> contentReceiver = vertx.eventBus().consumer(contentAddress);

      flowControl.setOnEndHandler(
          json -> {
            log.info("END: " + txId);
            // SHOULD BE: contentReceiver.unregister();
            // BUT A HACK: workaround for a bug: MessageConsumer.unregister() does NOT invoke endHandler
            ((HandlerRegistration) contentReceiver).unregister(true);
          }
      );

      flowControl.begin();
      future.complete(contentReceiver.bodyStream());
    }
    catch (Exception e) {
      future.fail(e);
    }
    streamHandler.handle(future);
  }
}
