package consumer;

import static commons.Config.ADAVA_KEDAVRA_ADDRESS;
import static commons.Config.BY;
import static commons.Config.STATUS;

import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

/**
 * The Dark Lord is watching ...
 */
public class DarkLord extends AbstractVerticle {

   private final Logger logger = Logger.getLogger(DarkLord.class.getName());

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("The Dark Lord is watching...");
      vertx.eventBus().<JsonObject>consumer(ADAVA_KEDAVRA_ADDRESS, message -> {
         JsonObject reboot = message.body();
         logger.info(("Status " + reboot.getString(STATUS) + " by " + reboot.getString(BY)));
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
