package org.cstamas.vertx.content;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.cstamas.vertx.content.ContentManager.require;

/**
 * Flow control between two endpoints.
 */
public class FlowControl
    implements Handler<Message<JsonObject>>
{
  private enum TxState
  {
    START, SEND, PAUSE, STOP;

    public TxState transition(TxTransition transition) {
      if (transition.from.contains(this)) {
        return transition.to;
      }
      else {
        throw new IllegalStateException("Invalid transition requested: " + this + "->" + transition.to);
      }
    }
  }

  private enum TxTransition
  {
    BEGIN(ImmutableSet.of(TxState.START), TxState.SEND),
    PAUSE(ImmutableSet.of(TxState.SEND), TxState.PAUSE),
    RESUME(ImmutableSet.of(TxState.PAUSE), TxState.SEND),
    END(ImmutableSet.of(TxState.SEND), TxState.STOP);

    private final Set<TxState> from;

    private final TxState to;

    TxTransition(final Set<TxState> from, final TxState to) {
      this.from = from;
      this.to = to;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FlowControl.class);

  private final Vertx vertx;

  private final String otherAddress;

  private final MessageConsumer<JsonObject> messageConsumer;

  private TxState state;

  private Handler<JsonObject> onBeginHandler;

  private Handler<JsonObject> onPauseHandler;

  private Handler<JsonObject> onResumeHandler;

  private Handler<JsonObject> onEndHandler;

  public FlowControl(final Vertx vertx,
                     final String myAddress,
                     final String otherAddress)
  {
    this.vertx = checkNotNull(vertx);
    this.otherAddress = checkNotNull(otherAddress);
    this.state = TxState.START;
    this.messageConsumer = vertx.eventBus().consumer(myAddress, this);
  }

  @Override
  public void handle(final Message<JsonObject> event) {
    final JsonObject message = event.body();
    TxTransition transition = TxTransition.valueOf(require(message, "transition"));
    state = state.transition(transition);
    switch (transition) {
      case BEGIN:
        if (onBeginHandler != null) {
          onBeginHandler.handle(message);
        }
        break;
      case PAUSE:
        if (onPauseHandler != null) {
          onPauseHandler.handle(message);
        }
        break;
      case RESUME:
        if (onResumeHandler != null) {
          onResumeHandler.handle(message);
        }
        break;
      case END:
        messageConsumer.unregister();
        if (onEndHandler != null) {
          onEndHandler.handle(message);
        }
        break;
    }
  }

  public FlowControl setOnBeginHandler(final Handler<JsonObject> onBeginHandler) {
    this.onBeginHandler = onBeginHandler;
    return this;
  }

  public FlowControl setOnPauseHandler(final Handler<JsonObject> onPauseHandler) {
    this.onPauseHandler = onPauseHandler;
    return this;
  }

  public FlowControl setOnResumeHandler(final Handler<JsonObject> onResumeHandler) {
    this.onResumeHandler = onResumeHandler;
    return this;
  }

  public FlowControl setOnEndHandler(final Handler<JsonObject> onEndHandler) {
    this.onEndHandler = onEndHandler;
    return this;
  }

  /**
   * Begins the flow, usually invoked by receiver when receiving end is set up.
   */
  public void begin() {
    transition(TxTransition.BEGIN);
  }

  /**
   * Pauses the flow, usually invoked by receiver.
   */
  public void pause() {
    transition(TxTransition.PAUSE);
  }

  /**
   * Resumes the flow, usually invoked by receiver.
   */
  public void resume() {
    transition(TxTransition.RESUME);
  }

  /**
   * Ends the flow, usually invoked by sender (at stream end).
   */
  public void end() {
    transition(TxTransition.END);
    messageConsumer.unregister();
  }

  private void transition(TxTransition transition) {
    log.info("TRANSITION " + state + "->" + transition.to);
    state = state.transition(transition);
    JsonObject jsonObject = new JsonObject()
        .put("transition", transition);
    vertx.eventBus().send(otherAddress, jsonObject);
  }
}
