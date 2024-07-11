package com.gracaconsultores.messages;

import com.gracaconsultores.messages.models.BigSerializedObject;
import com.gracaconsultores.messages.models.BigSerializedObjectCodec;
import com.gracaconsultores.messages.verticles.rest.HttpWebVerticle;
import com.gracaconsultores.messages.verticles.repository.DbConnVerticle;
import com.gracaconsultores.messages.verticles.messages.MessageVerticle;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MainVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);
    final JsonObject loadedConfig = new JsonObject();
    private static JsonObject env = new JsonObject();
    //private final Map<String, AtomicInteger> threadCounts;

    //MainVerticle(Map<String, AtomicInteger> threadCounts) {
    //this.threadCounts = threadCounts;
    //}
    @Override
    public void start(Promise<Void> start) {
      /*VertxOptions opts = new VertxOptions();
      opts.setEventLoopPoolSize(10);
      opts.setWorkerPoolSize(10);
      vertx = Vertx.vertx(opts);*/
      storeConfig(loadedConfig);
      deployOtherVerticles();
      vertx.eventBus().registerDefaultCodec(BigSerializedObject.class, new BigSerializedObjectCodec());
      start.complete();
    }

    Future<Void> storeConfig(JsonObject config) {
      log.info("storeConfig");
      return Future.<Void>future(promise -> loadedConfig.mergeIn(config));
    }


    Future<Void> deployOtherVerticles() {
      Promise<Void> promise = Promise.promise();
      List<Future<Void>> verticleDeployments = new ArrayList<>();

      ConfigStoreOptions defaultConfig = new ConfigStoreOptions()
        .setType("file")
        .setFormat("json")
        .setConfig(new JsonObject().put("path", "config.json"));
      ConfigStoreOptions cliConfig = new ConfigStoreOptions()
        .setType("json")
        .setConfig(config());

      ConfigRetrieverOptions optsC = new ConfigRetrieverOptions()
        .addStore(defaultConfig)
        .addStore(cliConfig);

      ConfigRetriever retriever = ConfigRetriever.create(vertx, optsC);

      retriever.getConfig().onComplete(ar -> {
        if (ar.failed()) {
          log.info("Failed to open file configure");
        } else {
          env = ar.result();
          log.info("env : " + env.toString());

          DeploymentOptions opts = new DeploymentOptions();
          opts.setConfig(env);
          opts.setInstances(1);

          verticleDeployments.add(deployHelper(HttpWebVerticle.class.getName(), opts));
          verticleDeployments.add(deployHelper(DbConnVerticle.class.getName(), opts));
          verticleDeployments.add(deployHelper(MessageVerticle.class.getName(),opts));

          CompositeFuture.all(new ArrayList<>(verticleDeployments)).onComplete(result -> {
            if (result.succeeded()) {
              System.out.println("[>] Verticles deployment complete");
              promise.complete();
            }
            else
              promise.fail(result.cause());
          });
          log.info("deployOtherVerticles");
        }
      });
      log.info("deployOtherVerticles");
      return promise.future();
      //return CompositeFuture.all(helloGroovy, helloJs, dbVerticle, webVerticle).mapEmpty();
    }
    private Future<Void> deployHelper(String name, DeploymentOptions opts) {
      Promise<Void> promise = Promise.promise();
      vertx.deployVerticle(name, opts, res -> {
        if(res.failed()) {
          System.out.println("[>] Failed to deploy verticle " + name);
          promise.fail(res.cause());
        } else {
          System.out.println("[>] Deployed verticle " + name);
          promise.complete();
        }
      });
      return promise.future();
    }

  public static void main(String[] args) {

    long startTime = System.currentTimeMillis();

    log.info("🚀 Starting a PostgreSQL container");

    // tag::tc-start[]
   /* PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:11-alpine")
      .withDatabaseName("postgres")
      .withUsername("postgres")
      .withPassword("vertx-in-action");*/

    //postgreSQLContainer.start();
    // end::tc-start[]

    long tcTime = System.currentTimeMillis();

    log.info("🚀 Starting Vert.x");

    // tag::vertx-start[]
    Vertx vertx = Vertx.vertx();

    DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject());
    //.put("pgPort", postgreSQLContainer.getMappedPort(5432))); // <1>

    vertx.deployVerticle(MainVerticle::new, options).onComplete(
      ok -> {
        long vertxTime = System.currentTimeMillis();
        log.info("✅ Deployment success");
        log.info("💡 PostgreSQL container started in {}ms", (tcTime - startTime));
        log.info("💡 Vert.x app started in {}ms", (vertxTime - tcTime));
      },
      err -> log.error("🔥 Deployment failure"));
    // end::vertx-start[]
  }
}
