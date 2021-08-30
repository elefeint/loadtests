package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import java.util.function.Supplier;
import javax.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebController {
  private static final int MAX_PRODUCT_ID = 1_000_000;
  private static Random rand = new Random();

  @Autowired
  private InventoryRepository inventoryRepository;

  @Autowired
  private Supplier<DatabaseClient> dbClient;


  //@Transactional
  @PostMapping("/update/{count}")
  public void updateAnyProduct(@PathVariable int count) throws Exception {
    int randomProductId = rand.nextInt(MAX_PRODUCT_ID);
    System.out.println("Updating product ID " + randomProductId);

    Thread.sleep(1000);
    inventoryRepository.updateInventory(count, randomProductId);
  }


  @PostMapping("/delete")
  public void deleteAtsInventoryStage() {
    TransactionRunner transactionRunner = dbClient.get().readWriteTransaction();
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
