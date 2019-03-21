package producer;

import static commons.Config.NAMED_ADDRESS;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Wizards daring to say the taboo name are being tracked by the curse
 */
public class Wizard extends AbstractVerticle {

   private final Logger logger = LoggerFactory.getLogger(Wizard.class);

   private final List<String> WIZARDS = Arrays.asList("Neville Longbottom", "Hermione Granger", "Ron Wesley");

   @Override
   public void start(Future<Void> startFuture) {
      logger.info("Wizard verticle started");
      vertx.setPeriodic(1000, x -> {
         int randomWizard = ThreadLocalRandom.current().nextInt(WIZARDS.size());
         String wizard = WIZARDS.get(randomWizard);
         logger.info(String.format("Wizard ** %s ** has said Voldemort", wizard));
         vertx.eventBus().send(NAMED_ADDRESS, wizard);
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
