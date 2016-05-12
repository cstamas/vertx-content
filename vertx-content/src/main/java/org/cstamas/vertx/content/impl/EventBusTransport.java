package org.cstamas.vertx.content.impl;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.FlowControl;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.ContentManager.require;
import static org.cstamas.vertx.content.ContentManager.txId;

/**
 * {@link Transport} implementation that uses {@link EventBus} for transport.
 */
public class EventBusTransport
    implements Transport
{
  private static final Logger log = LoggerFactory.getLogger(EventBusTransport.class);

  private static final String NAME = "eventBus";

  private final Vertx vertx;

  public EventBusTransport(final Vertx vertx) {
    this.vertx = checkNotNull(vertx);
  }

  @Override
  public String name() {
    return NAME;
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
        j -> {
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
    //flowControl.setOnPauseHandler(
    //    j -> {
    //      log.info("PAUSE: " + txId);
    //      stream.pause();
    //    }
    //);
    //flowControl.setOnResumeHandler(
    //    j -> {
    //      log.info("RESUME: " + txId);
    //      stream.resume();
    //    }
    //);
  }

  @Override
  public ReadStream<Buffer> receive(final JsonObject contentHandle, final FlowControl flowControl)
  {
    String txId = txId(contentHandle);
    String contentAddress = require(contentHandle, "contentAddress");
    MessageConsumer<Buffer> contentReceiver = vertx.eventBus().consumer(contentAddress);

    flowControl.setOnEndHandler(
        json -> {
          log.info("END: " + txId);
          contentReceiver.unregister();
        }
    );

    final ReadStream<Buffer> wireStream = contentReceiver.bodyStream();
    final ReadStream<Buffer> result = new ReadStream<Buffer>()
    {
      @Override
      public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
        wireStream.exceptionHandler(handler);
        return this;
      }

      @Override
      public ReadStream<Buffer> handler(final Handler<Buffer> handler) {
        wireStream.handler(handler);
        return this;
      }

      @Override
      public ReadStream<Buffer> pause() {
        wireStream.pause();
        flowControl.pause();
        return this;
      }

      @Override
      public ReadStream<Buffer> resume() {
        wireStream.resume();
        flowControl.resume();
        return this;
      }

      @Override
      public ReadStream<Buffer> endHandler(final Handler<Void> endHandler) {
        wireStream.endHandler(endHandler);
        return this;
      }
    };

    flowControl.begin();
    return result;
  }
}