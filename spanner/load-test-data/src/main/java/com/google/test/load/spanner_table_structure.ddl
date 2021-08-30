CREATE TABLE inventory_to_delete (
  uuid STRING(36),
  product_id INT64,
  product_count INT64
) PRIMARY KEY (uuid);