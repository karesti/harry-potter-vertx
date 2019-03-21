package consumer;

import static commons.Config.ADAVA_KEDAVRA_ADDRESS;
import static commons.Config.DEATH_EATER;
import static commons.Config.IS_BACK_FROM_CASTING;
import static commons.Config.IS_CASTING;
import static commons.Config.STATUS;
import static commons.Config.WIZARD;

import commons.Config;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Counter;

/**
 * Death Eaters monitor who is using the taboo name
 */
public class DeathEater extends AbstractVerticle {
   private final Logger logger = LoggerFactory.getLogger(DeathEater.class);
   private boolean isCastingAWizard = false;
   private Long id;

   @Override
   public void start(Future<Void> startFuture) {
      setIdAndStartTrackingWizards(startFuture, () ->
            vertx.eventBus().<String>consumer(Config.NAMED_ADDRESS, message -> {
               String wizardName = message.body();
               castWizard(wizardName);
            })
      );
   }

   private void castWizard(String wizard) {
      if (!isCastingAWizard) {
         isCastingAWizard = true;
         vertx.eventBus().send(ADAVA_KEDAVRA_ADDRESS, status(wizard, IS_CASTING));
         logger.info(">> cast started to " + wizard);

         vertx.setTimer(3000, h -> {
            vertx.eventBus().send(ADAVA_KEDAVRA_ADDRESS, status(wizard, IS_BACK_FROM_CASTING));
            logger.info("<< cast over " + wizard);
            isCastingAWizard = false;
         });
      }
   }

   private void setIdAndStartTrackingWizards(Future<Void> startFuture, Runnable tracking) {
      vertx.sharedData().getCounter("deathEaters", ar -> {
         if (ar.succeeded()) {
            Counter counter = ar.result();
            counter.incrementAndGet(inc -> {
               id = inc.result();
               logger.info("Death Eater " + id + " is ready to cast");
               tracking.run();
               startFuture.complete();
            });
         } else {
            logger.fatal(ar.cause());
         }
      });
   }

   private JsonObject status(String wizard, String isCasting) {
      JsonObject message = new JsonObject();
      message.put(STATUS, isCasting);
      message.put(DEATH_EATER, id.toString());
      message.put(WIZARD, wizard);
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
