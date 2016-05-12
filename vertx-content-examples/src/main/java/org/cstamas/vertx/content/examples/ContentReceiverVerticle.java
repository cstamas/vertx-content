package org.cstamas.vertx.content.examples;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import org.cstamas.vertx.content.ContentManager;

/**
 * Created by cstamas on 11/05/16.
 */
public class ContentReceiverVerticle
    extends AbstractVerticle
{
  private static final Logger log = LoggerFactory.getLogger(ContentReceiverVerticle.class);

  public static final String ADDRESS = "content.receiver";

  @Override
  public void start(final Future<Void> startFuture) throws Exception {
    ContentManager contentManager = ContentManager.http(vertx, new HttpServerOptions().setHost("localhost").setPort(8081));
    //ContentManager contentManager = ContentManager.eventBus(vertx);

    vertx.eventBus().consumer(
        ADDRESS,
        mh -> {
          JsonObject contentHandle = (JsonObject) mh.body();
          contentManager.receive(
              contentHandle,
              ch -> {
                if (ch.failed()) {
                  log.error("R: ", ch.cause());
                }
                else {
                  String path = contentHandle.getString("name") + ".received";
                  log.info("R: " + path);
                  vertx.fileSystem().open(
                      path,
                      new OpenOptions(),
                      oh -> {
                        if (oh.failed()) {
                          log.error("E:", oh.cause());
                        }
                        else {
                          Pump.pump(ch.result(), oh.result()).start();
                          ch.result().endHandler(h -> {
                            log.info("DONE");
                          });
                        }
                      }
                  );
                }
              }
          );
        }
    );
    super.start(startFuture);
  }
}
