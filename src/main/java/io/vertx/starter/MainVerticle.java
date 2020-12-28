package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import static org.jooq.impl.DSL.*;

import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.RowSet;
import org.jooq.*;
import org.jooq.impl.*;

public class MainVerticle extends AbstractVerticle {

  PgConnectOptions connectOptions;
  PoolOptions poolOptions;
  PgPool client;
  static String sql;


  @Override
  public void start() {
    connectOptions = new PgConnectOptions()
      .setPort(5432)
      .setHost("localhost")
      .setDatabase("authtest")
      .setUser("postgres")
      .setPassword("Asdf@1234");

    poolOptions = new PoolOptions()
      .setMaxSize(5);

    client = PgPool.pool(connectOptions, poolOptions);
    String sql;
    vertx.createHttpServer()
      .requestHandler(req -> {
        printJooqQuery(client);
        req.response().end("It is done.");
      })
      .listen(8080);
  }

  public void printJooqQuery(PgPool client){
    DSLContext create = DSL.using(SQLDialect.POSTGRES);
    Query query = create.select(field("BOOK.TITLE"), field("AUTHOR.FIRST_NAME"), field("AUTHOR.LAST_NAME"))
      .from(table("BOOK"))
      .join(table("AUTHOR"))
      .on(field("BOOK.AUTHOR_ID").eq(field("AUTHOR.ID")))
      .where(field("BOOK.PUBLISHED_IN").eq(1948));
    String sql = query.getSQL();
    System.out.println("Here's what you are trying: " + sql);
    client
      .query(sql)
      .execute(ar -> {
        if (ar.succeeded()) {
          RowSet<io.vertx.sqlclient.Row> result = ar.result();
          System.out.println("# of returned rows: " + result.size());
        } else {
          System.out.println("Oops that didn't work! Here's why\n" + ar.cause().getMessage());
        }
      });
    client.close();
  }
}
