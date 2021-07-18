package com.dbconnect.PostgresProject;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

public class MainVerticle {
	
	public static void main(String[] args) {
		
		Vertx vertx = Vertx.vertx();
//		RouterVerticle obj = new RouterVerticle();
//		vertx.deployVerticle(obj);
		
//		DbConnVerticle obj2 = new DbConnVerticle();
//		vertx.deployVerticle(obj2);
		
		DeploymentOptions options = new DeploymentOptions().setWorker(true);
		vertx.deployVerticle("package com.dbconnect.PostgresProject.DbConnVerticle", options);
		
	}
	
}
