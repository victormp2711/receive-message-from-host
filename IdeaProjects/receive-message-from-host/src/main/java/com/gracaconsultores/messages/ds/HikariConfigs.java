package com.gracaconsultores.messages.ds;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import criptografia.TripleDes;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;


import static io.vertx.core.impl.ConversionHelper.fromJsonObject;

@Slf4j
public class HikariConfigs extends AbstractVerticle {

    // Singleton instance
    private static volatile HikariDataSource dsInstance = null;
    private static String url;
    private static String user;
    private static String password;
    private static String passwordEnc;
    private static String componentOne;
    private static String componentTwo;
    private static int maxPool;
    private static int minIdle;
    private static String isPassworEncripted;
    private static String passwordDesc;

    private JDBCPool client;

    /**
     * Inicializa el datasource al arrancar el verticle.
     * Si ya está inicializado, no hace nada.
     */
    @Override
    public void start(Promise<Void> start) {
      ConfigRetriever retriever = ConfigRetriever.create(vertx);
      retriever.getConfig().onComplete(json -> {
        if (json.succeeded()){
          JsonObject env = json.result();
          Map map = fromJsonObject(env);
          Map ds1 = (Map) map.get("db_sic");
          url = (String) ds1.get("url");
          user = (String) ds1.get("user");
          password = (String) ds1.get("password");
          maxPool = (int) ds1.get("maxPool");
          minIdle = (int) ds1.get("minIdle");
          isPassworEncripted = (String) ds1.get("isPassworEncripted");
          try {
            getDataSource();
            log.info("✅ HikariConfigs is ready");
            start.complete();
          } catch (Exception e) {
            log.error("Error initializing datasource", e);
            start.fail(e);
          }
        } else {
          log.error("Error loading config for HikariCP", json.cause());
          start.fail(json.cause());
        }
      });
    }

    /**
     * Devuelve el singleton del datasource, inicializándolo si es necesario.
     * @return HikariDataSource
     */
    public static HikariDataSource getDataSource() {
      if (dsInstance == null) {
        synchronized (HikariConfigs.class) {
          if (dsInstance == null) {
            if (url == null || user == null || password == null) {
              throw new IllegalStateException("Datasource config not loaded yet");
            }
            dsInstance = createDataSource(url, user, password);
          }
        }
      }
      return dsInstance;
    }

    /**
     * Crea una instancia de HikariDataSource con la configuración proporcionada.
     */
    /*private JDBCPool createDataSourceJdbc(String url, String user, String pass) {
      try {
        JDBCConnectOptions connectOptions = new JDBCConnectOptions()
          .setJdbcUrl(url)
          .setUser(user)
          .setPassword(passwordDesc);


        PoolOptions poolOptions = new PoolOptions()
          .setMaxSize(10);

        client = JDBCPool.pool(vertx, connectOptions, poolOptions);
        startPromise.complete();
      } catch (Exception e) {
        log.error("Exception HikariConfig : ", e);
        throw new RuntimeException("Failed to initialize HikariCP datasource", e);
      }
    }*/

  private static HikariDataSource createDataSource(String url, String user, String pass) {
    try {
      log.info("--- > DataSource              :  {}", url);
      log.info("--- > user                    :  {}", user);
      log.info("--- > passwdE                 :  {}", pass);
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(url);
      config.setUsername(user);
      if(isPassworEncripted.startsWith("Y")){
        passwordDesc = getPasswordDecrypt(pass.trim(), "BDCustodioSic");
        log.info(passwordDesc);
        config.setPassword(passwordDesc);
      } else {
        config.setPassword(pass);
      }

      // Consolidar propiedades
      config.addDataSourceProperty("cachePrepStmts", "true");
      config.addDataSourceProperty("prepStmtCacheSize", "250");
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
      config.addDataSourceProperty("useServerPrepStmts", "true");
      config.addDataSourceProperty("cacheResultSetMetadata", "true");
      config.addDataSourceProperty("tcpKeepAlive", "true");
      config.setLeakDetectionThreshold(60 * 300000);
      config.setMaximumPoolSize(maxPool);
      config.setMinimumIdle(minIdle);
      config.setIdleTimeout(50000);
      config.setMaxLifetime(86000000);
      config.setConnectionInitSql("select 1 from dual");
      return new HikariDataSource(config);
    } catch (Exception e) {
      log.error("Exception HikariConfig : ", e);
      throw new RuntimeException("Failed to initialize HikariCP datasource", e);
    }
  }

    /**
     * Desencripta la contraseña usando TripleDes.
     */
    public static String getPasswordDecrypt(String encryptPassword, String groupKeys) {
      if (null != encryptPassword) {
        log.info("Desencriptando ... {}", encryptPassword);
        criptografia.TripleDes tdes = new criptografia.TripleDes();
        log.info("Grupo de Keys : keys/{}1/cl.xml", groupKeys);
        try {
          String password = tdes.descifrar("keys/" + groupKeys + "1/cl.xml", "keys/" + groupKeys + "2/cl.xml", encryptPassword);
          return password;
        } catch (Exception e) {
          log.debug("Error leyendo las llaves  {}", e.getMessage());
        }
        return null;
      } else {
        log.debug("password solicitado a desencriptar es nulo ");
        return encryptPassword;
      }
    }

    /**
     * Devuelve una conexión del pool. Lanza excepción si no está disponible.
     */
    public static Connection getConnection() throws SQLException {
      return getDataSource().getConnection();
    }
}
