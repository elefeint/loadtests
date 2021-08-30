package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * When /update/N HTTP POST endpoint is accessed, updates a randomly selected product_id with
 * product_count given through parameter.
 *
 */
@RestController
public class WebController {
  private static final int MAX_PRODUCT_ID = 1_000_000;
  private static Random rand = new Random();

  @Autowired
  private DatabaseClient dbClient;

  @PostMapping("/update/{count}")
  private void updateAnyProduct(@PathVariable int count) {
    int randomProductId = rand.nextInt(MAX_PRODUCT_ID);
    System.out.println("Updating product ID " + randomProductId);

    dbClient
        .readWriteTransaction()
        .run(ctx ->  {
          //Thread.sleep(1000); trying to simulate slow, blocking business logic

          ctx.executeUpdate(
                  Statement.newBuilder("UPDATE inventory SET product_count=@count WHERE product_id=@product_id")
                      .bind("count")
                      .to(count)
                      .bind("product_id")
                      .to(randomProductId)
                      .build());

          return null;
        });
  }


  @PostMapping("/delete")
  public void deleteAtsInventoryStage() {
    TransactionRunner transactionRunner = dbClient.readWriteTransaction();
    transactionRunner.run(ctx -> deleteLogic(ctx));
  }

  @GetMapping("/threaddump")
  public String  dumpThreadDump() {
    StringBuffer buffer = new StringBuffer();
    ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo ti : threadMxBean.dumpAllThreads(true, true)) {
      buffer.append(ti.toString());
    }
    return buffer.toString();
  }

  private long deleteLogic(TransactionContext ctx) {
    int randomProductId = rand.nextInt(110);
    randomProductId = 97;
    String query =
        "DELETE FROM inventory_to_delete " +
            "WHERE product_id IN (@productId) " +
            "      AND uuid IN (SELECT DISTINCT uuid " +
            "                   FROM inventory_to_delete " +
            "                   WHERE product_id IN (@productId) " +
            "                   LIMIT 5000 )";

    long before = System.currentTimeMillis();

    ctx.executeUpdate(
        Statement.newBuilder(query).bind("productId").to(randomProductId).build()
    );

    long after = System.currentTimeMillis();
    long timeMs = after - before;
    long timeInSeconds = timeMs / 1000;

    System.out.println("Deleting 5000 records took " + timeInSeconds + " seconds (" + (after - before) + " ms)");
    return timeMs;
  }
}
