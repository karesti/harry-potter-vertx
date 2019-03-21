package consumer;

import static commons.Config.ADAVA_KEDAVRA_ADDRESS;
import static commons.Config.DEATH_EATER;
import static commons.Config.IS_CASTING;
import static commons.Config.STATUS;
import static commons.Config.WIZARD;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Counter;

/**
 * The Dark Lord is watching ...
 */
public class DarkLord extends AbstractVerticle {

   private final Logger logger = LoggerFactory.getLogger(DarkLord.class);

   private boolean track;

   private Map<String, String> castsInProgress = new HashMap<>();

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("The Dark Lord is watching...");

      vertx.eventBus().<JsonObject>consumer(ADAVA_KEDAVRA_ADDRESS, message -> {
         JsonObject status = message.body();

         if (track) {
            logMessage(status);
         } else {
            checkDeathEatersAndLogMessage(() -> logMessage(status));
         }
      });
      startFuture.complete();
   }

   private void logMessage(JsonObject statusMessage) {
      String status = statusMessage.getString(STATUS);
      String deathEater = statusMessage.getString(DEATH_EATER);
      String wizard = statusMessage.getString(WIZARD);

      if (status.equals(IS_CASTING) && castsInProgress.containsKey(wizard)){
         logger.fatal(String.format("ALERT!!! %s started a cast, but %s is already being casted by %s", deathEater, wizard, castsInProgress.get(wizard)));
         return;
      }

      logger.info(String.format("%s %s %s", deathEater, status, wizard));
      if (track) {
         if (status.equals(IS_CASTING)) {
            castsInProgress.put(wizard, deathEater);
         } else {
            castsInProgress.remove(wizard);
         }
      }
   }

   private void checkDeathEatersAndLogMessage(Runnable r) {
      vertx.sharedData().getCounter("deathEaters", ar -> {
         if (ar.succeeded()) {
            Counter result = ar.result();
            result.get(car -> {
               if (car.succeeded()) {
                  Long deathEaters = car.result();
                  if (deathEaters > 1) {
                     track = true;
                  }
                  r.run();
               }
            });
         }
      });
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
