package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Creates 10M rows with each product_id associating with 90K rows.
 * Useful for running load tests operating on a large amount of rows at a time, while still being
 * somewhat disjoint (if different product_id is operated on).
 */
public class CreateTestDataForDeletion {

  final static Spanner spanner;
  final static DatabaseClient dbClient;

  static {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    spanner = options.getService();
    DatabaseId db = DatabaseId.of(options.getProjectId(), "loadtest", "store");
    dbClient = spanner.getDatabaseClient(db);
  }

  public static void main(String[] args) {

    int numRows = 10_000_000;
    int productSize = 90_000;
    int batchSize = 2_000;


    for (int i = 1; i <= numRows / productSize; i++) {
      System.out.println("Writing product " + i);

      for (int numBatchesWithSameProduct = 0; numBatchesWithSameProduct < productSize / batchSize; numBatchesWithSameProduct++) {
        System.out.println("  Writing batch " + numBatchesWithSameProduct);
        writeBatch(i, batchSize);
      }
    }

    spanner.close();

  }

  private static void writeBatch(int productId, int batchSize) {

    List<Mutation> mutations = new ArrayList<>();

    for (int i = 0; i < batchSize; i++) {
      mutations.add(
          Mutation.newInsertBuilder("inventory_to_delete")
              .set("uuid")
              .to(UUID.randomUUID().toString())
              .set("product_id")
              .to(productId)
              .set("product_count")
              .to(10)
              .build());
    }

    System.out.println("  Writing mutation batch of " + mutations.size());
    dbClient.write(mutations);

  }
}
