package com.dbconnect.PostgresProject;

import java.util.HashMap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;


public class RouterVerticle extends AbstractVerticle {
	

  @Override
  public void start() throws Exception {
    System.out.println("RouterVerticle is deployed");
    		
    HashMap<String,String> hm = new HashMap<String,String>();
    
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);
    
    //Route route = router.route(HttpMethod.GET, "/some/get/");
    router.route(HttpMethod.GET, "/student/:rollno/").handler(ctx -> {
		
		 HttpServerResponse response = ctx.response();
		 String roll = ctx.pathParam("rollno");
		 String name = hm.get(roll);
		 
		
		  response.setChunked(true);
		  if(name != null)
		  response.write("The name matching with your roll no. is :" +name);
		  else
		  response.write("no record found with the given roll number");  
		  ctx.response().end();
		});
    
    //Route route2 = router.route(HttpMethod.POST, "/some/post/");
    router.route(HttpMethod.POST, "/student/:rollno/:name/").handler(ctx -> {
		
		  HttpServerResponse response = ctx.response();
		 
		  response.setChunked(true);
		  String roll = ctx.pathParam("rollno");
		  String name = ctx.pathParam("name");
		  System.out.println(roll);
		  System.out.println(hm);
		  hm.put(roll, name);
		  response.write("Record Inserted Successfully\n" + hm);
		  ctx.response().end();
		});
    
    //Route route3 = router.route(HttpMethod.PUT, "/some/put/");
    router.route(HttpMethod.PUT, "/student/:rollno/:name/").handler(ctx -> {
		
		  HttpServerResponse response = ctx.response();
		 
		  response.setChunked(true);
		  
		  String roll = ctx.pathParam("rollno");
		  String name = ctx.pathParam("name");
		  hm.put(roll, name);
		  
		  response.write("Record Updated Successfully\n");
		  ctx.response().end();
		});
    
    //Route route4 = router.route(HttpMethod.DELETE, "/some/delete/");
    router.route(HttpMethod.DELETE, "/student/:roolno/").handler(ctx -> {
		
		  HttpServerResponse response = ctx.response();
		 
		  response.setChunked(true);
		  String roll = ctx.pathParam("rollno");
		  hm.remove(roll);
		  
		  response.write("Record deleted Successfully\n");
		  ctx.response().end();
		});
    
    
    
    server.requestHandler(router).listen(8888);
  }
  
  
 
}
