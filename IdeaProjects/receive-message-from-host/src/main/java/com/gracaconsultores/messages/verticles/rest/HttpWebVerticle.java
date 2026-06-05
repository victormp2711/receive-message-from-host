package com.gracaconsultores.messages.verticles.rest;

import com.gracaconsultores.messages.ds.HikariConfigs;
import com.gracaconsultores.messages.verticles.services.Service;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.gracaconsultores.messages.ds.SchedulerConfigs.accountsPilot;
import static com.gracaconsultores.messages.ds.SchedulerConfigs.info;
import static com.gracaconsultores.messages.verticles.repository.Repository.GET_ACCOUNT_FROM_COLLECT;
import static com.gracaconsultores.messages.verticles.services.Service.formatTimeDate;
import static io.vertx.core.impl.ConversionHelper.fromJsonObject;

public class HttpWebVerticle extends AbstractVerticle {

  private static final Logger log = LoggerFactory.getLogger(HttpWebVerticle.class);
  public static final String MESSAGE_FROM_HOST = "message.from.host";
  public static final JsonObject bodyResp = new JsonObject();
  private Router router;
  private Service service;

  public int port;
  public static JsonObject env = new JsonObject();
  private final String serviceId = "receive-message-from-host-batch";
  private WebClient webClient;


  @Override
  public void start() {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    vertx.eventBus().<JsonObject>consumer("new-configuration",  this::newConfig);
    webClient = WebClient.create(vertx);

    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        env = json.result();
        service = new Service();

        router = Router.router(vertx);
        //router.route().handler(BodyHandler.create());
        // Serve static resources from the /assets directory
        router.post("/api/messageFromHost").consumes("application/json").handler(this::messageFromHost);
        router.get("/api/testDb").handler(service::testDb);
        router.get("/api/getStatistics").handler(service::getStatistics);
        router.get("/api/cleanMemoryAccount").handler(service::cleanMemoryAccount);
        router.get("/api/getMemoryAccount").handler(service::getMemoryAccount);

        Map map = fromJsonObject(env);
        Map ports = (Map) map.get("http");
        port = (int) ports.get("port");
        log.info("port : " + port);
        int registreInDiscovery = ports.get("registreWhitDiscovery") == null ?  0 : (int) ports.get("registreWhitDiscovery");
        //int registreInDiscovery = value == 1 ? 1 : 0;
        log.debug("registreInDiscovery : " + registreInDiscovery);

        vertx.createHttpServer().requestHandler(router)
          .listen(port, result -> {
            if (result.succeeded()) {
              log.debug("✅ HTTP server running on port {}", result.result().actualPort());
              if(registreInDiscovery == 1) {
                registerWithDiscovery();
                startDiscoveryCheck();
              }
              bodyResp.put("code", 1000).put("message", "success");
            } else {
              log.error("Could not start a HTTP server ", result.cause());
            }
          });
        //log.debug("✅ webService is ready" );
      } else {
        log.error("Could not load config enviroment ", json.cause());
      }
    });
  }

  public void messageFromHostOld(RoutingContext context) {
    log.debug("Begin messageFromHost : " + context.getBodyAsString());
    context.request().bodyHandler((bodyHandler) -> {
      JsonObject jsonBody;
      try {
        jsonBody = bodyHandler.toJsonObject();
        if (!jsonBody.containsKey("account") || jsonBody.getString("account").length() < 20) {
          respondWithError(context, 400, "Missing or invalid 'account' field");
          return;
        } else if (!jsonBody.containsKey("account") || jsonBody.getString("account").length() < 20) {
          respondWithError(context, 400, "Missing or invalid 'account' field");
          return;
        } else {
          context.json(bodyResp); // Respuesta inmediata intencional
          //jsonBody = jsonBody.put("originId", UUID.randomUUID().toString()).put("origin", "PIC");
          vertx.eventBus().request(MESSAGE_FROM_HOST, jsonBody, reply -> {
            if (reply.succeeded()) {
              log.info("proceso terminado correctamente");
            } else {
              log.info("error en messageFromHost : " + reply.cause());
            }
          });
        }
      } catch (Exception e) {
        respondWithError(context, 400, "Invalid JSON body");
        return;
      }
    });
  }


  public void messageFromHost(RoutingContext context) {
    log.debug("Begin messageFromHost : " + context.getBodyAsString());
    context.request().bodyHandler((bodyHandler) -> {
      JsonObject jsonBody = bodyHandler.toJsonObject();
      jsonBody = jsonBody.put("originId", UUID.randomUUID().toString()).put("origin", "PIC_BATCH");
      vertx.eventBus().request(MESSAGE_FROM_HOST, jsonBody, reply -> {
        if (reply.succeeded()) {
          log.debug("✅ accountBalance success");
          context.json(reply.result().body());
        } else {
          context.json(reply.result().body());
          log.info("error en messageFromHost : " + reply.result().body());
        }
      });
    });
  }
  private  void newConfig(Message<JsonObject> msg) {
    //log.debug("received new chage cofiguration in : " + msg.body());
    JsonObject jsonConfig = (JsonObject) msg.body();
    env = jsonConfig;
  }

  private void healthCheck(RoutingContext context) {
    JsonObject health = new JsonObject()
      .put("status", "UP")
      .put("service", serviceId)
      .put("port", port)
      .put("users_count", 10)
      .put("timestamp", System.currentTimeMillis());

    context.response()
      .putHeader("Content-Type", "application/json")
      .end(health.encode());
  }

  private void startDiscoveryCheck() {
    vertx.setPeriodic(30000, id -> checkRegistration());
  }

  private void checkRegistration() {
    if (serviceId == null) return;
    webClient.get(80, "discovery-service.sic", "/service/" + serviceId)
      .send(ar -> {
        if (ar.succeeded()) {
          if (ar.result().statusCode() == 404) {
            log.warn("Este servicio ya no está registrado en Discovery Service.");
            // (Opcional) Intentar re-registrarse automáticamente
            registerWithDiscovery();
          }
        } else {
          log.warn("No se pudo verificar el registro en Discovery Service: " + ar.cause().getMessage());
        }
      });
  }

  private void registerWithDiscovery() {
    JsonObject serviceInfo = new JsonObject()
      .put("id", serviceId)
      .put("name", serviceId)
      .put("host", serviceId+".sic")
      .put("port", port)
      .put("basePath", "/api/*")
      .put("metadata", new JsonObject()
        .put("version", "1.0.0")
        .put("description", "Servicio de gestión de usuarios"));

    webClient.post(80, "discovery-service.sic", "/register")
      .sendJsonObject(serviceInfo)
      .onSuccess(response -> {
        if (response.statusCode() == 200 || response.statusCode() == 201) {
          System.out.println("✅ User Service registrado en Discovery Service");
        } else {
          System.err.println("❌ Error registrando en Discovery: " + response.statusCode());
        }
      })
      .onFailure(error -> {
        System.err.println("❌ Error conectando con Discovery Service: " + error.getMessage());
      });
  }

  private void respondWithError(RoutingContext context, int status, String message) {
    context.response().setStatusCode(status).putHeader("Content-Type", "application/json")
            .end(new JsonObject().put("code", status).put("message", message).encode());
  }
}
