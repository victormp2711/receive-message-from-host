package com.dbconnect.PostgresProject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.oracleclient.OracleConnectOptions;
import io.vertx.oracleclient.OraclePool;
import io.vertx.sqlclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbConnVerticle extends AbstractVerticle{

  private final Logger log = LoggerFactory.getLogger( DbConnVerticle.class );
  public static final String TEST_ADDR = "hello.vertx.addr";

		public void start() {
      configureEventBusConsumers();
		}


    private OraclePool getPool(){
      OracleConnectOptions  connectOptions = new OracleConnectOptions()
        .setPort(1560)
        .setHost("180.183.199.191")
        .setServiceName("PCPQ.BANVENEZ.CORP")
        .setUser("PCP")
        .setPassword("TU65Y5283Ua1");

      // Pool options
      PoolOptions poolOptions = new PoolOptions()
        .setMaxSize(5);

      // Create the client pool
      OraclePool client = OraclePool.pool(vertx, connectOptions, poolOptions);
      return  client;
    }

    Future<Void> configureEventBusConsumers() {
      return Future.<Void>future(promise -> {
        vertx.eventBus().consumer(TEST_ADDR, (message) -> {
          System.out.println(message.body().toString().length());
          //test(message);
        });
        vertx.eventBus().consumer("incoming.message.articles", this::listAllArticles2);
        //vertx.eventBus().consumer(TEST_ADDR).handler(this::test);
      });
    }

  private <T> void  listAllArticles2(io.vertx.core.eventbus.Message<Object> msg) {
    log.info("Listing all articles2");
    getPool().query("SELECT * FROM articles").execute()
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
        getPool().close();
      });
  }

    /*void test(Message<Object> msg) {
      log.info("Begin test : " + msg.toString());
      getPool()
        .query("SELECT * FROM articles ORDER BY id ASC ")
        .execute(ar -> {
          if (ar.succeeded()) {
            RowSet<Row> result = ar.result();
            log.info("Got " + result.size() + " rows ");
            BigSerializedObject messageObject = new BigSerializedObject();
            JsonArray jsonArray = new JsonArray();
            result.forEach(row -> {
              JsonObject ob = row.toJson();
              jsonArray.add(ob);
            });
            log.info("End test :" + jsonArray.toString());
            msg.reply(jsonArray.toString());
            //msg.reply(messageObject.toString());
          } else {
            log.info("Failure: " + ar.cause().getMessage());
          }
          // Now close the pool
          getPool().close();
        });
    }*/
}
