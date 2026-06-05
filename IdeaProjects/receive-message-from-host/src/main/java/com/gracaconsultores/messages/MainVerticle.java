package com.gracaconsultores.messages;

import com.gracaconsultores.messages.ds.HikariConfigs;
import com.gracaconsultores.messages.ds.SchedulerConfigs;
import com.gracaconsultores.messages.verticles.generic.service.GenericChargeService;
import com.gracaconsultores.messages.verticles.repository.*;
import com.gracaconsultores.messages.verticles.asset.service.AssetCharge;
import com.gracaconsultores.messages.verticles.asset.service.AssetChargePic;
import com.gracaconsultores.messages.verticles.generic.service.GenericCharge;
import com.gracaconsultores.messages.verticles.rest.HttpWebVerticle;
import com.gracaconsultores.messages.verticles.services.Service;
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
  public void start(Promise<Void> promise) throws InterruptedException {
    storeConfig(loadedConfig);
    deployOtherVerticles();
    promise.complete();
  }

  void storeConfig(JsonObject config) {
    log.info("storeConfig");
    Future.<Void>future(promise -> loadedConfig.mergeIn(config));
  }


  void deployOtherVerticles() throws InterruptedException {
    Promise<Void> promise = Promise.promise();
    List<Future> verticleDeployments = new ArrayList<>();

    ConfigStoreOptions defaultConfig = new ConfigStoreOptions()
      .setType("file")
      .setFormat("json")
      .setConfig(new JsonObject().put("path", "properties/sic/receive-message-from-host-batch.json"));

    ConfigStoreOptions configDb = new ConfigStoreOptions()
      .setType("file")
      .setFormat("json")
      .setConfig(new JsonObject().put("path", "properties/sic/database.json"));

    ConfigStoreOptions cliConfig = new ConfigStoreOptions()
      .setType("json")
      .setConfig(config());

    ConfigRetrieverOptions optsC = new ConfigRetrieverOptions()
      .addStore(defaultConfig)
      .addStore(configDb)
      .addStore(cliConfig);

    ConfigRetriever retriever = ConfigRetriever.create(vertx, optsC);

    retriever.getConfig().onComplete(ar -> {
      if (ar.failed()) {
        log.info("Failed to open file configure");
      } else {
        env = ar.result();
        //log.info("env : " + env.toString());

        DeploymentOptions optsInstance = new DeploymentOptions();
        optsInstance.setConfig(env);
        optsInstance.setThreadingModel(ThreadingModel.EVENT_LOOP);
        optsInstance.setInstances(20);

        DeploymentOptions optsInstance2 = new DeploymentOptions();
        optsInstance2.setConfig(env);
        optsInstance2.setThreadingModel(ThreadingModel.EVENT_LOOP);
        optsInstance2.setInstances(1);

        DeploymentOptions optsWorker = new DeploymentOptions();
        optsWorker.setConfig(env);
        optsWorker.setThreadingModel(ThreadingModel.WORKER);
        optsWorker.setWorkerPoolName("worker-service");
        optsWorker.setInstances(20);
        optsWorker.setWorkerPoolSize(20);

        DeploymentOptions optsWorker2 = new DeploymentOptions();
        optsWorker2.setConfig(env);
        optsWorker2.setThreadingModel(ThreadingModel.WORKER);
        optsWorker2.setWorkerPoolName("worker-repository");
        optsWorker2.setInstances(20);
        optsWorker2.setWorkerPoolSize(20);

        DeploymentOptions optsWorker3 = new DeploymentOptions();
        optsWorker3.setConfig(env);
        optsWorker3.setThreadingModel(ThreadingModel.WORKER);
        optsWorker3.setWorkerPoolName("worker-accounts");
        optsWorker3.setInstances(20);
        optsWorker3.setWorkerPoolSize(20);

        DeploymentOptions optsWorker4 = new DeploymentOptions();
        optsWorker4.setConfig(env);
        optsWorker4.setThreadingModel(ThreadingModel.WORKER);
        optsWorker4.setWorkerPoolName("worker-logs");
        optsWorker4.setInstances(20);
        optsWorker4.setWorkerPoolSize(20);

        DeploymentOptions optsWorker5 = new DeploymentOptions();
        optsWorker5.setConfig(env);
        optsWorker5.setThreadingModel(ThreadingModel.WORKER);
        optsWorker5.setWorkerPoolName("worker-generic");
        optsWorker5.setInstances(20);
        optsWorker5.setWorkerPoolSize(20);

        DeploymentOptions optsWorker7 = new DeploymentOptions();
        optsWorker7.setConfig(env);
        optsWorker7.setThreadingModel(ThreadingModel.WORKER);
        optsWorker7.setWorkerPoolName("worker-asset2");
        optsWorker7.setInstances(20);
        optsWorker7.setWorkerPoolSize(20);

        DeploymentOptions optsWorker8 = new DeploymentOptions();
        optsWorker8.setConfig(env);
        optsWorker8.setThreadingModel(ThreadingModel.WORKER);
        optsWorker8.setWorkerPoolName("worker-asset2-pic");
        optsWorker8.setInstances(20);
        optsWorker8.setWorkerPoolSize(20);

        DeploymentOptions optsWorker6 = new DeploymentOptions();
        optsWorker6.setConfig(env);
        optsWorker6.setThreadingModel(ThreadingModel.WORKER);
        optsWorker6.setWorkerPoolName("worker-SchedulerConfigs");
        optsWorker6.setInstances(1);
        optsWorker6.setWorkerPoolSize(1);

        Future<String> hikariFuture = vertx.deployVerticle(HikariConfigs.class.getName(), optsInstance2);

        // Luego desplegar SchedulerConfigs y los demás verticles
        hikariFuture.onComplete(hikariResult -> {
          if (hikariResult.failed()) {
            log.error("Failed to deploy HikariConfigs", hikariResult.cause());
            promise.fail(hikariResult.cause());
          } else {
            log.info("HikariConfigs deployed successfully");

            // Desplegar SchedulerConfigs después de HikariConfigs
            verticleDeployments.add(deployHelper(SchedulerConfigs.class.getName(), optsWorker6));

            // Desplegar el resto de los verticles
            verticleDeployments.add(deployHelper(HttpWebVerticle.class.getName(), optsInstance));
            verticleDeployments.add(deployHelper(Service.class.getName(), optsWorker));
            verticleDeployments.add(deployHelper(Repository.class.getName(), optsWorker2));
            verticleDeployments.add(deployHelper(RepositoryAccounts.class.getName(), optsWorker3));
            verticleDeployments.add(deployHelper(RepositoryLog.class.getName(), optsWorker4));
            verticleDeployments.add(deployHelper(GenericCharge.class.getName(), optsWorker5));
            verticleDeployments.add(deployHelper(AssetCharge.class.getName(), optsWorker7));
            verticleDeployments.add(deployHelper(AssetChargePic.class.getName(), optsWorker8));

            CompositeFuture.all(verticleDeployments).onComplete(result -> {
              if (result.succeeded()) {
                log.info("[>] Verticles deployment complete");
                promise.complete();
              } else {
                log.error("Failed to deploy verticles", result.cause());
                promise.fail(result.cause());
              }
            });
          }
        });
      }
    });

    retriever.listen(change -> {
      JsonObject json = change.getNewConfiguration();
      vertx.eventBus().publish("new-configuration", json);
    });
    promise.future();
    //return CompositeFuture.all(helloGroovy, helloJs, dbVerticle, webVerticle).mapEmpty();
  }

  private Future<Void> deployHelper(String name, DeploymentOptions opts) {
    Promise<Void> promise = Promise.promise();
    vertx.deployVerticle(name, opts, res -> {
      if(res.failed()) {
        System.out.println("[>] Failed to deploy verticle " + name + " : " + res.cause());
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
    long tcTime = System.currentTimeMillis();
    log.info("🚀 Starting Vert.x");
    Vertx vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject());

    vertx.deployVerticle(MainVerticle::new, options).onComplete(
      ok -> {
        long vertxTime = System.currentTimeMillis();
        log.info("✅ Deployment success");
        log.info("💡 Vert.x app started in {}ms", (vertxTime - tcTime));
      },
      err -> log.error("🔥 Deployment failure"));
  }
}
