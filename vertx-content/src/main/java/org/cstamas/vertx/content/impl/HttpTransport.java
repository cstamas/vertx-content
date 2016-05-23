package org.cstamas.vertx.content.impl;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import org.cstamas.vertx.content.Transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.impl.ContentManagerImpl.require;
import static org.cstamas.vertx.content.impl.ContentManagerImpl.txId;

/**
 * {@link Transport} implementation that uses {@link HttpServer} and {@link HttpClient} for transport.
 */
public class HttpTransport
    implements Transport
{
  private static final Logger log = LoggerFactory.getLogger(HttpTransport.class);

  private final Vertx vertx;

  private final Map<String, ReadStream<Buffer>> contents;

  private final HttpServerOptions httpServerOptions;

  private final HttpServer httpServer;

  public HttpTransport(final Vertx vertx, final HttpServerOptions httpServerOptions) {
    this.vertx = checkNotNull(vertx);
    this.contents = new HashMap<>();
    this.httpServerOptions = httpServerOptions;
    this.httpServer = createServer();
    log.info("Created " + getClass().getSimpleName() + " (" + httpServerOptions.getHost() + ":" +
        httpServerOptions.getPort() + ")");
  }

  private HttpServer createServer() {
    return vertx.createHttpServer(httpServerOptions)
        .requestHandler(
            req -> {
              String txId = req.path().substring(1);
              log.debug("REQ: " + req.method() + " " + txId);
              ReadStream<Buffer> content = null;
              synchronized (contents) {
                content = contents.remove(txId);
              }
              if (content == null) {
                req.response().setStatusCode(404).end();
              }
              else {
                HttpServerResponse response = req.response();
                content.endHandler(v -> response.end());
                response
                    .setStatusCode(200)
                    .setChunked(true)
                    .putHeader("Content-Type", "application/octet-stream");
                Pump.pump(content, response).start();
              }
            }
        ).listen();
  }

  @Override
  public void send(final JsonObject contentHandle,
                   final ReadStream<Buffer> stream)
  {
    String txId = txId(contentHandle);
    synchronized (contents) {
      checkArgument(!contents.containsKey(txId), "Content txId already exists", txId);
      contents.put(txId, stream);
    }
    String url = String.format(
        "http://%s:%s/%s",
        httpServerOptions.getHost(),
        httpServerOptions.getPort(),
        txId
    );
    contentHandle.put("url", url);
  }

  @Override
  public void receive(final JsonObject contentHandle,
                      final Handler<AsyncResult<ReadStream<Buffer>>> streamHandler)
  {
    String txId = txId(contentHandle);
    URI url = URI.create(require(contentHandle, "url"));
    HttpClient client = vertx.createHttpClient();
    client.get(
        url.getPort(),
        url.getHost(),
        "/" + txId,
        resp -> {
          if (log.isDebugEnabled()) {
            log.debug("HTTP Resp: " + resp.statusCode() + " " + resp.statusMessage());
            resp.headers().forEach(e -> log.debug(e.getKey() + " : " + e.getValue()));
          }

          resp.endHandler(
              v -> {
                client.close();
              }
          );
          if (resp.statusCode() == 200) {
            ReadStream<Buffer> result = new ReadStream<Buffer>()
            {
              @Override
              public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
                resp.exceptionHandler(handler);
                return this;
              }

              @Override
              public ReadStream<Buffer> handler(final Handler<Buffer> handler) {
                resp.handler(handler);
                return this;
              }

              @Override
              public ReadStream<Buffer> pause() {
                resp.pause();
                return this;
              }

              @Override
              public ReadStream<Buffer> resume() {
                resp.resume();
                return this;
              }

              @Override
              public ReadStream<Buffer> endHandler(final Handler<Void> endHandler) {
                resp.endHandler(
                    v -> {
                      client.close();
                      endHandler.handle(v);
                    }
                );
                return this;
              }
            };
            streamHandler.handle(Future.succeededFuture(result));
          }
          else {
            streamHandler
                .handle(Future.failedFuture(new IllegalArgumentException("Unexpected response " + resp.statusCode())));
          }
        }
    ).end();
  }
}

