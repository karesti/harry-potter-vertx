package producer;

import static commons.Config.NAMED;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class Wizard extends AbstractVerticle {

   private final Logger logger = Logger.getLogger(Wizard.class.getName());

   private final List<String> WIZARDS = List.of("Alice Longbottom", "Albus Dumbledore", "James Potter");

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("Wizard verticle started");

      vertx.setPeriodic(2000, x -> {
         int randomWizard = ThreadLocalRandom.current().nextInt(WIZARDS.size());
         String wizard = WIZARDS.get(randomWizard);
         logger.info(String.format("Wizard %s has said Voldemort", wizard));
         vertx.eventBus().send(NAMED, wizard);
      });
      startFuture.complete();
   }

   public static void main(String[] args) {
      Vertx.clusteredVertx(new VertxOptions().setClustered(true), ar -> {
         if (ar.failed()) {
            System.err.println("Cannot create vert.x instance : " + ar.cause());
         } else {
            Vertx vertx = ar.result();
            vertx.deployVerticle(Wizard.class.getName());
         }
      });
   }
}
