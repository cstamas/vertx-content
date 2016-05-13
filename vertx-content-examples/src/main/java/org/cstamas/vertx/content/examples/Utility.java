package org.cstamas.vertx.content.examples;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import org.cstamas.vertx.content.ContentManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class Utility
{
  private Utility() {
    // nop
  }

  public static ContentManager create(final Vertx vertx, final JsonObject config) {
    final String type = config.getString("type");
    checkArgument(!isNullOrEmpty(type), "null type");
    if ("eventbus".equals(type)) {
      return ContentManager.eventBus(vertx);
    }
    else if ("http".equals(type)) {
      final String host = config.getString("host");
      final int port = config.getInteger("port");
      return ContentManager.http(vertx, new HttpServerOptions().setHost(host).setPort(port));
    }
    else {
      throw new IllegalArgumentException("Unknown manager type: " + type);
    }
  }
}
