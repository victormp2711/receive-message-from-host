package com.gracaconsultores.messages.verticles.repository;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.oracleclient.OracleConnectOptions;
import io.vertx.oracleclient.OraclePool;
import io.vertx.sqlclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.vertx.core.impl.ConversionHelper.fromJsonObject;

public class DbConnVerticle1 extends AbstractVerticle{

  private final Logger log = LoggerFactory.getLogger( DbConnVerticle1.class );
  public static final String TEST_ADDR = "hello.vertx.addr";
  public static final String MESSAGE_FROM_HOST = "message.from.host";
  public static final String DOMIC_CHARGE = "message.domic.charge";
  public static final String ASSET_CHARGE = "message.asset.charge";
  public static final String getAccountFromSicDomic = "SELECT ACCOUNT_NUMBER FROM SIC_DEBT_ACCOUNTS WHERE ID_DEBT_COLLECTOR = 3 and DEBT_STATUS = 1 AND ACCOUNT_NUMBER = ?";
  public static final String getAccountFromSicAsset = "SELECT ACCOUNT_NUMBER, CREDIT_NUMBER, CURRENCY FROM SIC_DEBT_ACCOUNTS WHERE ID_DEBT_COLLECTOR = 2 and DEBT_STATUS = 1 AND ACCOUNT_NUMBER = ?";
  public static final String getPendingChargesFromDomic = "SELECT\n" +
    "    NVL(D.ID, 0) AS ID,\n" +
    "    D.IDHEADER,\n" +
    "    D.RIF_COBRADOR,\n" +
    "    D.DIGITO_COBRADOR,\n" +
    "    NVL(D.MONTO_PENDIENTE_COBRAR, 0) AS MONTO_PENDIENTE_COBRAR,\n" +
    "    NVL(D.CI_RIF, 'V00000000') AS CI_RIF,\n" +
    "    NVL(D.DIG_VERIF, '0') AS DIG_VERIF,\n" +
    "    LPAD(NVL(D.SERIAL_DEBITO_CUENTA, '0'),5,'0') AS SERIAL_DEBITO_CUENTA,\n" +
    "    D.TIPO_PAGO_ACTUAL,\n" +
    "    D.CODIGO_EMPRESA,\n" +
    "    LPAD(NVL(D.REF_DEBITO, '1'),20,'0') AS REF_DEBITO,\n" +
    "    LPAD(NVL(D.NRO_CTA_1, '0'),20,'0') AS NRO_CTA_1,\n" +
    "    LPAD(NVL(D.NRO_CTA_2, '0'),20,'0') AS NRO_CTA_2,\n" +
    "    D.COD_CLIENT,\n" +
    "    D.STATUS\n" +
    "FROM\n" +
    "    EN_PAY_HOME_HEADER H,\n" +
    "    EN_PAY_HOME_DETAIL D\n" +
    "WHERE\n" +
    "    D.IDHEADER = H.ID\n" +
    "    AND H.STATUS IN ('LT005','LT004')\n" +
    "    AND D.STATUS IN ('RG004','RG005','RG007','RG008')\n" +
    "    AND D.FORMA_DE_PAGO = '01'\n" +
    "    AND D.FLAG_PROCESO_HOST = '00'\n" +
    "    AND D.MONTO_PENDIENTE_COBRAR > 0\n" +
    "    AND TRUNC(SYSDATE) >= D.FECHA_VALOR\n" +
    "    AND TRUNC(SYSDATE) <= D.FECHA_CULMINACION\n" +
    "    AND LPAD(NVL(D.NRO_CTA_1, '0'),20,'0') = ?";

  private static String hostDomic;
  private static int portDomic;
  private static String serviceNameDomic;
  private static String userDomic;
  private static String passwordDomic;

  private static String hostSic;
  private static int portSic;
  private static String serviceNameSic;
  private static String userSic;
  private static String passwordSic;

	public void start() {
      configureEventBusConsumers();
      connectionDbSic();
  }


  Future<Void> connectionDbSic() {
    return Future.<Void>future(promise -> {
      ConfigRetriever retriever = ConfigRetriever.create(vertx);
      retriever.getConfig().onComplete(json -> {
        if (json.succeeded()){
          JsonObject env = json.result();
          //log.info("result : " + env);
          Map map = fromJsonObject(env);
          //log.info("map : " + map.toString());
          Map ds = (Map) map.get("db_sic");
          //log.info("ports : " +  ports.get("port_qa"));
          hostSic = (String) ds.get("host");
          portSic = Integer.valueOf((String) ds.get("port"));
          serviceNameSic = (String) ds.get("service_name");
          userSic = (String) ds.get("user");
          passwordSic = (String) ds.get("password");
          log.info("✅ connection to sic database is ready");
        } else {
          log.error("🔥Could not load config enviroment ", json.cause());
        }

      });
      //vertx.eventBus().consumer(TEST_ADDR).handler(this::test);
    });
  }
    private OraclePool getPoolDomic(){
      OracleConnectOptions  connectOptions = new OracleConnectOptions()
        .setPort(portDomic)
        .setHost(hostDomic)
        .setServiceName(serviceNameDomic)
        .setUser(userDomic)
        .setPassword(passwordDomic);

      // Pool options
      PoolOptions poolOptions = new PoolOptions()
        .setMaxSize(15);

      // Create the client pool
      OraclePool client = OraclePool.pool(vertx, connectOptions, poolOptions);
      return  client;
    }

    private OraclePool getPoolSic(){
      OracleConnectOptions  connectOptions = new OracleConnectOptions()
        .setPort(portSic)
        .setHost(hostSic)
        .setServiceName(serviceNameSic)
        .setUser(userSic)
        .setPassword(passwordSic);

      // Pool options
      PoolOptions poolOptions = new PoolOptions()
        .setMaxSize(15);

      // Create the client pool
      OraclePool client = OraclePool.pool(vertx, connectOptions, poolOptions);
      return  client;
    }

    void configureEventBusConsumers() {
      Future.<Void>future(promise -> {
        vertx.eventBus().consumer("incoming.message.articles", this::listAllArticles2);
        vertx.eventBus().consumer(MESSAGE_FROM_HOST).handler(this::processMessage);
      });
    }

  private <T> void  listAllArticles2(io.vertx.core.eventbus.Message<Object> msg) {
    log.info("Listing all articles2");
    getPoolDomic().query("SELECT * FROM articles").execute()
      .onComplete(rs -> {
        if (rs.succeeded()) {
          JsonArray ar = new JsonArray();
          RowIterator<Row> it = rs.result().iterator();
          while (it.hasNext()) {
            Row row = it.next();
            JsonObject o = new JsonObject();
            for (int i = 0; i < row.size(); i++) {
              o.put(row.getColumnName(i), row.getValue(i));
            }
            ar.add(o);
          }
          log.info("resultado : " + ar.toString());
          msg.reply(ar);
        } else {
          System.out.println("Failure: " + rs.cause().getMessage());
          msg.fail(500, rs.cause().getLocalizedMessage());
        }
        getPoolDomic().close();
      }).onFailure(h -> {getPoolDomic().close();});
  }

  private <T> void processMessage(Message<T> tMessage) {
    log.info("Begin processMessage : " + tMessage.body());
    JsonObject bodyIn = (JsonObject) tMessage.body();
    //Future.all(getAccountFromSicDomic(bodyIn), getAccountFromSicAsset(bodyIn)).onComplete(ar -> {
    getAccountFromSicDomic(bodyIn).onComplete(ar ->{
      if (ar.succeeded()) {
        getAccountFromSicAsset(bodyIn).onComplete(ar1 ->{
            if (ar1.succeeded()){
              log.info("resultado getAccountFromSicAsset : " + ar.toString());
              tMessage.reply(new JsonObject()
                .put("code", 1000)
                .put("message", "success")
                .put("status", 200));
            } else {
              log.info("Failure call to asset: " + ar.cause().getMessage());
              tMessage.reply(new JsonObject()
                .put("code", 1020)
                .put("message", "error" + ar.cause().getMessage())
                .put("status", 200));
            }
        });
      }  else {
        log.info("Failure call to domic: " + ar.cause().getMessage());
        tMessage.reply(new JsonObject()
          .put("code", 1020)
          .put("message", "error" + ar.cause().getMessage())
          .put("status", 200));
      }
    });
  }

  private Future<?> getAccountFromSicDomic(JsonObject jsonIn) {
    log.info("getAccountFromSicDomic : " + jsonIn.toString());
    return getPoolSic().preparedQuery(getAccountFromSicDomic).execute(Tuple.of(jsonIn.getString("account")))
      .onComplete(rs -> {
        if (rs.succeeded()) {
          RowSet<Row> result = rs.result();
          if (result.size() > 0) {
            RowIterator<Row> it = rs.result().iterator();
            Row row = it.next();
            JsonObject o = new JsonObject();
            o.put(row.getColumnName(0), row.getValue(0));
            log.info("resultado : " + o.toString());
            getPoolSic().close();
            log.info("query domic : " + getPendingChargesFromDomic);
            getPoolDomic().preparedQuery(getPendingChargesFromDomic).execute(Tuple.of(jsonIn.getString("account")))
              .onComplete(rs2 -> {
                if (rs2.succeeded()) {
                  RowSet<Row> result2 = rs2.result();
                  if (result2.size() > 0) {
                    JsonArray arr = new JsonArray();
                    RowIterator<Row> it2 = rs2.result().iterator();
                    while (it2.hasNext()) {
                      Row row2 = it2.next();
                      JsonObject o2 = new JsonObject();
                      for (int i = 0; i < row2.size(); i++) {
                        o2.put(row2.getColumnName(i), row2.getValue(i));
                      }
                      arr.add(o2);
                    }
                    getPoolDomic().close();
                    log.info("pending charges from domic: " + arr.toString());
                    arr.stream().forEach(rowArr ->{
                      vertx.eventBus().request(DOMIC_CHARGE, rowArr, reply -> {
                        if (reply.succeeded()) {
                          log.info("✅ messageFromHost success");
                          reply.result().body();
                        } else {
                          log.info("No reply");
                        }
                      });
                    });
                  } else {
                    log.info("records not found in for domic");
                  }
                } else {
                  log.error("Failure rs2: " + rs2.cause().getMessage());
                  getPoolDomic().close();
                  rs2.cause().printStackTrace();
                }
              });
          } else {
            log.info("account no found in sic for domic");
          }
        } else {
          log.error("Failure rs : " + rs.cause().getMessage());
          rs.cause().printStackTrace();
        }
        getPoolSic().close();
        log.info("End  getAccountFromSicDomic");
      }).onFailure(h -> {
            getPoolDomic().close();
          });
  }

  private Future<?> getAccountFromSicAsset(JsonObject jsonIn) {
    log.info("Begin getAccountFromSicAsset : " + jsonIn);
    return getPoolSic().preparedQuery(getAccountFromSicAsset).execute(Tuple.of(jsonIn.getString("account")))
      .onComplete(rs -> {
        if (rs.succeeded()) {
          log.info("succeeded account for asset");
          RowSet<Row> result = rs.result();
          if (result.size() > 0) {
            log.info("recodrs for account " + jsonIn.getString("account") + " of asset is " + result.size());
            JsonArray arr = new JsonArray();
            RowIterator<Row> it = rs.result().iterator();
            while (it.hasNext()) {
              Row row = it.next();
              JsonObject o = new JsonObject();
              for (int i = 0; i < row.size(); i++) {
                o.put(row.getColumnName(i), row.getValue(i));
              }
              arr.add(o);
            }
            getPoolSic().close();
            log.info("processing charge to asset : " + arr.toString());
            arr.stream().forEach(rowArr ->{
              vertx.eventBus().request(ASSET_CHARGE, rowArr, reply -> {
                if (reply.succeeded()) {
                  log.info("✅ messageFromHost success");
                  reply.result().body();
                } else {
                  log.info("No reply by : " + reply.cause());
                }
              });
            });
          } else {
            log.info("records not found for asset by account: " + jsonIn.getString("account"));
            log.info("End  getAccountFromSicAsset");
            getPoolDomic().close();
          }
        } else {
          System.out.println("Failure: " + rs.cause().getMessage());
        }
        log.info("End  getAccountFromSicAsset");
        getPoolSic().close();
      }).onFailure(h -> {getPoolDomic().close();});
  }
}
