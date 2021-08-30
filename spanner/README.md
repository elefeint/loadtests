This is my load testing setup. I did not externalize any hardcoded variables, so run search-replace on:
* instance name: s/loadtest/[YOUR_INSTANCE_HERE]
* database name: s/store/[YOUR_DB_HERE]

The table structure being operated on is in `load-test-data/.../spanner_table_structure.ddl`


There are 2 web applications:
* spanner-web-app is a pure client library
* spanner-spring-data-web-app uses Spring Cloud GCP's Spring Data integration for Spanner