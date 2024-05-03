package com.dbconnect.PostgresProject;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MainVerticle extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);
    final JsonObject loadedConfig = new JsonObject();
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
      DeploymentOptions opts = new DeploymentOptions();

      verticleDeployments.add(deployHelper(HttpWebVerticle.class.getName(), opts));
      verticleDeployments.add(deployHelper(DbConnVerticle.class.getName(), opts));
      verticleDeployments.add(deployHelper(MessageVerticle.class.getName(),opts));
      //verticleDeployments.add(deployHelper("src/main/resource/Hello.js",opts));


      CompositeFuture.all(new ArrayList<>(verticleDeployments)).onComplete(result -> {
        if (result.succeeded()) {
          System.out.println("[>] Verticles deployment complete");
          promise.complete();
        }
        else
          promise.fail(result.cause());
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
}
