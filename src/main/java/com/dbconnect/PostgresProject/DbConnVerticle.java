package com.dbconnect.PostgresProject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;

public class DbConnVerticle extends AbstractVerticle{

		public void start() throws Exception {
	
			
			 PgConnectOptions connectOptions = new PgConnectOptions()
		    		  .setPort(5432)
		    		  .setHost("localhost")
		    		  .setDatabase("student")
		    		  .setUser("postgres")
		    		  .setPassword("pass@123");

		    		// Pool options
		    		PoolOptions poolOptions = new PoolOptions()
		    		  .setMaxSize(5);
		    		
		    		// Create the client pool
		    		SqlClient client = PgPool.client(vertx, connectOptions, poolOptions);

		    		// A simple query
		    		
		    		client
		    		  .query("select * from student_info")
		    		  .execute(ar -> {
		    		  if (ar.succeeded()) {
		    		    RowSet<Row> result = ar.result();
		    		    System.out.println("Got " + result.size() + " rows ");
		    		    
		   
		    		    
		    		    for (Row row : result) {
		    		        System.out.println("row = " + row.toJson());
		    		        JsonObject ob = row.toJson();
		    		        System.out.println(ob.getString("Name"));
		    		      }
		    		    
		    		  } else {
		    		    System.out.println("Failure: " + ar.cause().getMessage());
		    		  }
		    		  
		    		  

		    		  // Now close the pool
		    		  client.close();
		    		});
		}
}
