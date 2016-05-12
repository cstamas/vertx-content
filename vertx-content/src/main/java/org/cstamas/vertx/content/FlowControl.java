package org.cstamas.vertx.content;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 * Flow control between two endpoints.
 */
public interface FlowControl
{
  /**
   * Handler invoked on begin of the flow.
   */
  FlowControl setOnBeginHandler(Handler<JsonObject> onBeginHandler);

  /**
   * Handler invoked on pause of the flow.
   */
  FlowControl setOnPauseHandler(Handler<JsonObject> onPauseHandler);

  /**
   * Handler invoked on resume of the flow.
   */
  FlowControl setOnResumeHandler(Handler<JsonObject> onResumeHandler);

  /**
   * Handler invoked on end of the flow.
   */
  FlowControl setOnEndHandler(Handler<JsonObject> onEndHandler);

  /**
   * Begins the flow, usually invoked by receiver when receiving end is set up.
   */
  void begin();

  /**
   * Pauses the flow, usually invoked by receiver.
   */
  void pause();

  /**
   * Resumes the flow, usually invoked by receiver.
   */
  void resume();

  /**
   * Ends the flow, usually invoked by sender (at stream end).
   */
  void end();
}
