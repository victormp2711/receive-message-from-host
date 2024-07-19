package com.gracaconsultores.messages.verticles.repository;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.OracleTypes;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.vertx.core.impl.ConversionHelper.fromObject;

@Slf4j
public class DbConnVerticle extends AbstractVerticle {

  private static String host;
  private static int port;
  private static String serviceName;
  private static String user;
  private static String password;
  private static String dir;
  private static String dirBkp;
  private static int bcsaPort;
  private static String bcsaUrl;
  private static String bcsaIp;
  private static List<Map> collectors;
  public static final String STORE_PROCEDURE_ASSET = "store.procedure.asset.addr";
  public static final String MESSAGE_FROM_HOST = "message.from.host";
  public static final String STORE_PROCEDURE_DOMIC = "store.procedure.domic.addr";
  public static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

  @Override
  public void start(Promise<Void> start) {
    //vertx.setPeriodic(1000 * 60 * 60, (l) -> { -- cada hora
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig().onComplete(json -> {
      if (json.succeeded()){
        JsonObject env = json.result();
        //log.info("result : " + env);
        Map map = fromJsonObject(env);
        //log.info("map : " + map.toString());
        Map ds = (Map) map.get("db_sic");
        //log.info("ports : " +  ports.get("port_qa"));
        host = (String) ds.get("host");
        port = Integer.valueOf((String) ds.get("port"));
        serviceName = (String) ds.get("service_name");
        user = (String) ds.get("user");
        password = (String) ds.get("password");
        /* obteniendo rutas de las propiedades */
        Map ds2 = (Map) map.get("scheduler");
        //log.info("ports : " +  ports.get("port_qa"));
        dir = (String) ds.get("dir");
        dirBkp = (String) ds.get("dirBkp");

        Map ds3 = (Map) map.get("bcsa");
        bcsaUrl = (String) ds3.get("url");
        bcsaIp = (String) ds3.get("ip");
        bcsaPort = (int) ds3.get("port");
        /* obteniendo las propiedades de los cobradores */
        collectors = (List<Map>) map.get("collectors");
        log.info("collectors : " + collectors);
        //log.info("collectors 0  : " + collectors.get(0));
        //log.info("collectors 1 : " + collectors.get(1).get("url"));
      }
    });
    log.info("executing process storeProcesureAsset ");
    eventBusConsumersAccount();
  }

  void eventBusConsumersAccount() {
    Future.<Void>future(promise -> {
      vertx.eventBus().consumer(MESSAGE_FROM_HOST).handler(this::getAccountFromCollect);
    });
  }

  public static Map<String, Object> fromJsonObject(JsonObject json) {
    if (json == null) {
      return null;
    }
    Map<String, Object> map = new LinkedHashMap<>(json.getMap());
    map.entrySet().forEach(entry -> {
      entry.setValue(fromObject(entry.getValue()));
    });
    return map;
  }
  private Connection getConnectionSic() {
    //Connection conn = null;
    try {
      Class.forName("oracle.jdbc.OracleDriver");
      String connection = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName;
      log.info("connection : " + connection);
      return DriverManager.getConnection(
        "jdbc:oracle:thin:@" + host + ":" + port + "/" + serviceName,
        user, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  private Future<?> getAccountFromCollect(Message<Object> msg) {
    log.info("Begin getAccountFromCollect : " + msg.body());
    JsonObject jsonIn = (JsonObject) msg.body();
    final CallableStatement[] cStmt = {null};
    return Future.<Void>future(promise -> {
      Connection conn = getConnectionSic();
      try {
        if (conn != null) {
          log.info("- llamando al store procedure");
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNT_FOR_ALL_COLLECTORS(?, ?, ?)}");
          log.info("seteado el call al cStmt");
          cStmt[0].setString("P_IN_ACCOUNT", jsonIn.getString("account"));
          log.info("seteado el P_IN_ACCOUNT al cStmt");
          cStmt[0].setString("P_IN_COLLECTOR", jsonIn.getString("collector"));
          log.info("seteado el P_IN_COLLECTOR al cStmt");
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          log.info("seteado el P_OUT_DATA al cStmt");
          //cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          //log.info("seteado el P_OUT_STATUS al cStmt");
          log.info("antes de ejecutar cStmt");
          cStmt[0].execute();
          log.info("despues de ejecutar cStmt");
          String status = null;
          status = (String) cStmt[0].getObject("P_OUT_STATUS");
          log.info("status: " + status);
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");

          JsonArray arrayJson = new JsonArray();
          while (rs.next()) {
            String accountNumber  = rs.getString("ACCOUNT_NUMBER");
            int idCollector       = rs.getInt("ID_DEBT_COLLECTOR");
            String priority       = rs.getString("PRIORITY");
            JsonObject obj = new JsonObject();
            obj.put("accountNumber", accountNumber);
            obj.put("idCollector", idCollector);
            obj.put("priority", priority);
            arrayJson.add(obj);
          }
          conn.close();
          log.info("- resultado de datos: " + arrayJson.toString());

          arrayJson.forEach(item -> {
            JsonObject obj = (JsonObject) item;
            switch (obj.getInteger("idCollector")) {
              case 2 ->   // procesar cobranza de Activo
                assetCharge(obj);

                    /*case 3:   // procesar cobranza de domiciliacion y otros productos
                      genericCharge(new JsonObject()
                      .put("account", accountNumber));
                      break;*/
              default -> {
                log.info("procesando default process");
                genericCharge(obj);  // procesar cobranza de domiciliacion y otros productos
              }
            }
          });
          msg.reply(new JsonObject()
            .put("code", 1000)
            .put("message", "success")
            .put("status", 200));
        } else {
          msg.reply(new JsonObject()
            .put("code", 1020)
            .put("message", "error : " + "connecxion retornada es null")
            .put("status", 200));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally{
        try {
          if(cStmt[0] !=null) cStmt[0].close(); //close CallableStatement
          if(conn!=null) conn.close(); // close connection
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private Future<?> getAccountFromCollect1(Message<Object> msg) {
    log.info("Begin getAccountFromCollect : " + msg.body());
    JsonObject jsonIn = (JsonObject) msg.body();
    final CallableStatement[] cStmt = {null};
    return Future.<Void>future(promise -> {
      Connection conn = getConnectionSic();
      try {
        if (conn != null) {
          log.info("- llamando al store procedure");
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNT_FOR_COLLECTORS(?, ?, ?)}");
          log.info("seteado el call al cStmt");
          cStmt[0].setString("P_IN_ACCOUNT", jsonIn.getString("account"));
          log.info("seteado el P_IN_ACCOUNT al cStmt");
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          log.info("seteado el P_OUT_DATA al cStmt");
          cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          log.info("seteado el P_OUT_STATUS al cStmt");
          log.info("antes de ejecutar cStmt");
          cStmt[0].execute();
          log.info("despues de ejecutar cStmt");
          String status = null;
          status = (String) cStmt[0].getObject("P_OUT_STATUS");
          log.info("status: " + status);
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");

          JsonArray arrayJson = new JsonArray();
          while (rs.next()) {
            String accountNumber  = rs.getString("ACCOUNT_NUMBER");
            int idCollector       = rs.getInt("ID_DEBT_COLLECTOR");
            String priority       = rs.getString("PRIORITY");
            JsonObject obj = new JsonObject();
            obj.put("accountNumber", accountNumber);
            obj.put("idCollector", idCollector);
            obj.put("priority", priority);
            arrayJson.add(obj);
          }
          conn.close();
          log.info("- resultado de datos: " + arrayJson.toString());

          arrayJson.forEach(item -> {
            JsonObject obj = (JsonObject) item;
            switch (obj.getInteger("idCollector")) {
              case 2 ->   // procesar cobranza de Activo
                assetCharge(obj);

                    /*case 3:   // procesar cobranza de domiciliacion y otros productos
                      genericCharge(new JsonObject()
                      .put("account", accountNumber));
                      break;*/
              default -> {
                log.info("procesando default process");
                genericCharge(obj);  // procesar cobranza de domiciliacion y otros productos
              }
            }
          });
          msg.reply(new JsonObject()
            .put("code", 1000)
            .put("message", "success")
            .put("status", 200));
        } else {
          msg.reply(new JsonObject()
            .put("code", 1020)
            .put("message", "error : " + "connecxion retornada es null")
            .put("status", 200));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally{
        try {
          if(cStmt[0] !=null) cStmt[0].close(); //close CallableStatement
          if(conn!=null) conn.close(); // close connection
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private Future<JsonObject> getAccountsByCollector(JsonObject jsonIn) {
    log.info("🚀 getAccountsByCollector : " + jsonIn);
    final CallableStatement[] cStmt = {null};
    return Future.<JsonObject>future(promise -> {
      Connection conn = getConnectionSic();
      try {
        if (conn != null) {
          cStmt[0] = conn.prepareCall("{CALL GIOM.PKG_SIC_DEBT_ACCOUNTS_COLLECTOR.GET_ACCOUNT_BY_COLLECTOR(?, ?, ?, ?)}");
          cStmt[0].setString("P_IN_ACCOUNT", jsonIn.getString("account"));
          cStmt[0].setInt("P_IN_COLLECTOR", jsonIn.getInteger("collector"));
          cStmt[0].registerOutParameter("P_OUT_DATA", OracleTypes.REF_CURSOR);
          cStmt[0].registerOutParameter("P_OUT_STATUS", OracleTypes.VARCHAR);
          cStmt[0].execute();
          String status = null;
          status = (String) cStmt[0].getObject("P_OUT_STATUS");
          log.info("status: " + status);
          final ResultSet rs = (ResultSet) cStmt[0].getObject("P_OUT_DATA");
          rs.next();
          JsonArray arrayJson = new JsonArray();
          JsonObject obj = new JsonObject();
          obj.put("accountNumber", rs.getString("ACCOUNT_NUMBER"));
          obj.put("creditNumber", rs.getInt("CREDIT_NUMBER"));
          obj.put("currency", rs.getString("CURRENCY"));
          conn.close();
        } else {
          log.info("message", "error : " + "connecxion retornada es null");
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally{
        try {
          if(cStmt[0] !=null) cStmt[0].close(); //close CallableStatement
          if(conn!=null) conn.close(); // close connection
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private <T> void genericCharge(JsonObject bodyIn) {
    log.info("🚀 genericCharge : " + bodyIn);
    WebClient client = WebClient.create(vertx);
    accountBalance(bodyIn).onComplete(bcsa -> {
      if(bcsa.succeeded()) {
        JsonObject bcsaR = bcsa.result().bodyAsJsonObject();
        log.info("bcsac asset response : " + bcsaR);
        log.info("bcsac code  : " + bcsaR.getInteger("code"));
        if(bcsaR.getInteger("code") == 1000) {
          JsonObject jsonMap = bcsaR.getJsonObject("data").getJsonObject("BGMCSA1");
          log.info("jsonMap : " + jsonMap);
          log.info("building asset  charge U087 and comparing amount balance : " + jsonMap.getFloat("ppalSdoFinal"));
          if (jsonMap.getFloat("ppalSdoFinal") > 0.0) {
            log.info("sending json to charge to collector : " + bodyIn.getString("idCollector") + " : " + bodyIn);
            bodyIn.put("url", collectors.get(bodyIn.getInteger("idCollector")).get("url"));
            bodyIn.put("ip", collectors.get(bodyIn.getInteger("idCollector")).get("ip"));
            bodyIn.put("port", collectors.get(bodyIn.getInteger("idCollector")).get("port"));
            chargeGeneric(bodyIn).onComplete(charge ->{
              if (charge.succeeded()) {
                JsonObject response = charge.result().bodyAsJsonObject();
                log.info("response : " + response);
                if(response.getInteger("status") == 1000){
                  log.info("✅ return service with HTTP response with status of collector " + bodyIn.getInteger("idCollector")  + response.getString("message"));
                } else {
                  log.info("no se realiza la operacion de cobranza activos por : " + response.getString("message"));
                }
              } else {
                charge.cause().printStackTrace();
                log.info("no se realiza la operacion de cobranza activos por : " + charge.cause().getMessage());
              }
            });
          } else {
            log.info("no se envia la operacion de cobranza por falta de saldo");
          }
        } else {
          log.info("no se envia la operacion de cobranza por : " + bcsaR.getString("message"));
        }
      } else {
        log.info("no se envia la operacion de cobranza por falla : " + bcsa.cause());
      }
    });
  }

  private <T> void assetCharge(JsonObject bodyIn) {
    log.info("🚀 assetCharge : " + bodyIn);
    WebClient client = WebClient.create(vertx);
    JsonObject bodyInCollector = new JsonObject()
      .put("account", bodyIn.getValue("accountNumber"))
      .put("collector", "idCollector");
    getAccountsByCollector(bodyInCollector).onComplete(ar ->{
        if(ar.succeeded()) {
          JsonObject jsonResponse = ar.result();
          log.info("se obtiene datos complementarios para la U087: " + jsonResponse);
          JsonObject bodyInCharge = new JsonObject();
          bodyInCharge.put("contratoCredito",   jsonResponse.getString("CREDIT_NUMBER"));
          bodyInCharge.put("codDivisa",         jsonResponse.getString("CURRENCY"));
          bodyInCharge.put("recibos",           "");
          bodyInCharge.put("importe",           0.00);
          bodyInCharge.put("fechaValor",        sdf2.format(new Date()));
          bodyInCharge.put("indFormaDePago",    "1");
          bodyInCharge.put("cccCargo",          bodyIn.getString("ACCOUNT_NUMBER"));
          bodyInCharge.put("numeroDeCheque",    "");
          bodyInCharge.put("importeDelCheque",  "");
          bodyInCharge.put("tasaDeMora",        "");
          bodyInCharge.put("importeDeMora",     "");
          bodyInCharge.put("nioDeCobroLinea",   "");
          bodyInCharge.put("indicadorDeCobro",  "");
          bodyInCharge.put("user",              "BDVN001");
          log.info("sending json to asset charge pic U087 : " + bodyInCharge);
          assetChargePic(bodyIn, bodyInCharge).onComplete(charge ->{
            if (charge.succeeded()) {
              JsonObject responseU087 = charge.result().bodyAsJsonObject();
              log.info("responseU087 : " + responseU087);
              if(responseU087.getInteger("status") == 1000){
                log.info("✅ return pic with HTTP response with status " + responseU087.getString("message"));
              } else {
                log.info("no se realiza la operacion de cobranza activos por : " + responseU087.getString("message"));
              }
            } else {
              log.info("error al realiza la operacion de cobranza activos por : " + charge.cause());
            }
          });
        } else {
          log.info("ne se pudo obtener datos complementarios para la U087: " + bodyInCollector);
        }
    }).onFailure(f -> log.error("error al realiza la operacion de getAccountsByCollector con : " + bodyInCollector));
  }

  Future<HttpResponse<Buffer>> accountBalance(JsonObject jsonIn) {
    log.info("🚀 AccountBalance PIC received message : " + jsonIn);
    WebClientOptions options = new WebClientOptions()
      .setConnectTimeout(5000)
      .setUserAgent("My-App/1.2.3");
    options.setKeepAlive(false);
    //WebClient client = WebClient.create(vertx, options);
    WebClient client = WebClient.create(vertx, options);
    JsonObject bodyIn = new JsonObject()
      .put("codigoCtaCliente", jsonIn.getValue("ACCOUNT_NUMBER"))
      .put("divisa", "VES");
    log.info("Sending json : " + bodyIn);
    log.info("to : " + bcsaPort + " - " + bcsaIp + " - " + bcsaUrl);

      /*return Future.future(promise ->
        client.post(bcsaPort, bcsaIp, bcsaUrl).sendJson(bodyIn).compose(response -> {return response.bodyAsJson(response.body());})
      );*/
    return client.post(bcsaPort, bcsaIp, bcsaUrl).sendJson(bodyIn);
  }

  private Future<HttpResponse<Buffer>> assetChargePic(JsonObject jsonIn, JsonObject bodyIn) {
    Promise<JsonObject> promise = Promise.promise();
    log.info("🚀 assetChargePic : " + jsonIn);
    WebClientOptions options = new WebClientOptions()
      .setConnectTimeout(5000)
      .setUserAgent("Pic-App/1.2.3");
    options.setKeepAlive(false);
    //WebClient client = WebClient.create(vertx, options);
    WebClient client = WebClient.create(vertx, options);
      /*JsonObject bodyIn = new JsonObject()
        .put("codigoCtaCliente", jsonIn.getValue("NRO_CTA_1"))
        .put("divisa", "VES");*/
    log.info("Sending json : " + jsonIn);

    return client.post(jsonIn.getInteger("port"), jsonIn.getString("ip"), jsonIn.getString("url")).sendJson(bodyIn);
  }

  Future<HttpResponse<Buffer>> chargeGeneric(JsonObject jsonIn) {
    log.info("🚀 chargeGeneric : " + jsonIn);
    WebClientOptions options = new WebClientOptions()
      .setConnectTimeout(5000)
      .setUserAgent("My-App/1.2.3");
    options.setKeepAlive(false);
    //WebClient client = WebClient.create(vertx, options);
    WebClient client = WebClient.create(vertx, options);
    JsonObject bodyIn = new JsonObject()
      .put("codigoCtaCliente", jsonIn.getValue("ACCOUNT_NUMBER"))
      .put("divisa", "VES");
    log.info("Sending json : " + bodyIn);
    log.info("to : " + jsonIn.getInteger("port") + " - " + jsonIn.getString("ip") + " - " + jsonIn.getString("url"));
    return client.post(jsonIn.getInteger("port"), jsonIn.getString("ip"), jsonIn.getString("url")).sendJson(bodyIn);
  }
}
