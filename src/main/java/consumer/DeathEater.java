package consumer;

import static commons.Config.ADAVA_KEDAVRA_ADDRESS;
import static commons.Config.BY;
import static commons.Config.STATUS;

import java.util.UUID;
import java.util.logging.Logger;

import commons.Config;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;

public class DeathEater extends AbstractVerticle {

   private final Logger logger = Logger.getLogger(DeathEater.class.getName());
   private boolean isCastingAWizard = false;
   private String id = UUID.randomUUID().toString().substring(0, 4).toUpperCase() + "_DE";

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("Death Eater verticle " + id + " started");
      vertx.eventBus().<String>consumer(Config.NAMED, message -> {
         String wizardName = message.body();
         launchReboot(wizardName);
      });

      startFuture.complete();
   }

   private void launchReboot(String wizard) {
      if (!isCastingAWizard) {
         isCastingAWizard = true;
         vertx.eventBus().send(ADAVA_KEDAVRA_ADDRESS, startRebootMessage(wizard));
         logger.info(">> cast started to " + wizard);

         vertx.setTimer(3000, h -> {
            vertx.eventBus().send(ADAVA_KEDAVRA_ADDRESS, endRebootMessage());
            logger.info("<< cast over " + wizard);
            isCastingAWizard = false;
         });
      }
   }

   private JsonObject startRebootMessage(String wizard) {
      JsonObject message = new JsonObject();
      message.put(STATUS, "Cast " + wizard);
      message.put(BY, id);
      return message;
   }

   private JsonObject endRebootMessage() {
      JsonObject message = new JsonObject();
      message.put(STATUS, "Coming back");
      message.put(BY, id);
      return message;
   }

   public static void main(String[] args) {
      VertxOptions vertxOptions = new VertxOptions().setClustered(true);
      Vertx.clusteredVertx(vertxOptions, ar -> {
         if (ar.failed()) {
            System.err.println("Cannot create vert.x instance : " + ar.cause());
         } else {
            Vertx vertx = ar.result();
            vertx.deployVerticle(DeathEater.class.getName());
         }
      });
   }
}
