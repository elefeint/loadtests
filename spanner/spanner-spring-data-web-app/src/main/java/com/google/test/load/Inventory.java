package com.google.test.load;

import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;

public class Inventory {

  public Inventory(String uuid, int productId, int productCount) {
    this.uuid = uuid;
    this.productId = productId;
    this.productCount = productCount;
  }

  @PrimaryKey
  private String uuid;

  private int productId;

  private int productCount;

}
