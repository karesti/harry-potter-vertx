package consumer;

import static commons.Config.ADAVA_KEDAVRA_ADDRESS;
import static commons.Config.BY;
import static commons.Config.STATUS;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * The Dark Lord is watching ...
 */
public class DarkLord extends AbstractVerticle {

   private final Logger logger = LoggerFactory.getLogger(DarkLord.class);

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("The Dark Lord is watching...");
      vertx.eventBus().<JsonObject>consumer(ADAVA_KEDAVRA_ADDRESS, message -> {
         JsonObject reboot = message.body();
         logger.info(String.format("%s %s", reboot.getString(BY), reboot.getString(STATUS)));
      });
      startFuture.complete();
   }

   public static void main(String[] args) {
      VertxOptions vertxOptions = new VertxOptions().setClustered(true);
      Vertx.clusteredVertx(vertxOptions, ar -> {
         if (ar.failed()) {
            System.err.println("Cannot create vert.x instance : " + ar.cause());
         } else {
            Vertx vertx = ar.result();
            vertx.deployVerticle(DarkLord.class.getName());
         }
      });
   }
}
