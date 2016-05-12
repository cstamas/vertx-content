package org.cstamas.vertx.content.impl;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.FlowControl;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.ContentManager.txId;

/**
 * {@link Transport} implementation that uses {@link HttpServer} and {@link HttpClient} for transport.
 */
public class HttpTransport
    implements Transport
{
  private static final Logger log = LoggerFactory.getLogger(HttpTransport.class);

  private static final String NAME = "http";

  private final Vertx vertx;

  public HttpTransport(final Vertx vertx) {
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
    // TODO: HTTP Server should publish resource (copied to tmp or just as "single shot"?) and send the URL
  }

  @Override
  public ReadStream<Buffer> receive(final JsonObject contentHandle, final FlowControl flowControl)
  {
    String txId = txId(contentHandle);
    // TODO: client should GET the content
    return null;
  }
}
