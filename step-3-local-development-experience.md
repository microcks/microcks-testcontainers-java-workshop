# Step 3: Local development experience with Microcks

Our application uses Kafka and external dependencies.

Currently, if you run the application from your terminal, you will see the following error:

```shell
./mvnw spring-boot:run  

[INFO] Attaching agents: []

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/

 :: Spring Boot ::                (v3.4.7)

2026-06-01T10:49:12.035+02:00  INFO 46316 --- [  restartedMain] org.acme.order.OrderServiceApplication   : Starting OrderServiceApplication using Java 21.0.9 with PID 46316 (/Users/laurent/Development/github/microcks-testcontainers-java-workshop/target/classes started by laurent in /Users/laurent/Development/github/microcks-testcontainers-java-workshop)
2026-06-01T10:49:12.036+02:00  INFO 46316 --- [  restartedMain] org.acme.order.OrderServiceApplication   : No active profile set, falling back to 1 default profile: "default"
[...]
2026-06-01T10:49:12.758+02:00  INFO 46316 --- [  restartedMain] o.a.kafka.common.utils.AppInfoParser     : Kafka version: 3.8.1
2026-06-01T10:49:12.758+02:00  INFO 46316 --- [  restartedMain] o.a.kafka.common.utils.AppInfoParser     : Kafka commitId: 70d6ff42debf7e17
2026-06-01T10:49:12.758+02:00  INFO 46316 --- [  restartedMain] o.a.kafka.common.utils.AppInfoParser     : Kafka startTimeMs: 1780303752757
2026-06-01T10:49:12.759+02:00  INFO 46316 --- [  restartedMain] o.a.k.c.c.internals.LegacyKafkaConsumer  : [Consumer clientId=consumer-order-service-1, groupId=order-service] Subscribed to topic(s): orders-reviewed
2026-06-01T10:49:12.766+02:00  INFO 46316 --- [  restartedMain] org.acme.order.OrderServiceApplication   : Started OrderServiceApplication in 0.853 seconds (process running for 1.018)
2026-06-01T10:49:12.837+02:00  INFO 46316 --- [ntainer#0-0-C-1] org.apache.kafka.clients.NetworkClient   : [Consumer clientId=consumer-order-service-1, groupId=order-service] Node -1 disconnected.
2026-06-01T10:49:12.837+02:00  WARN 46316 --- [ntainer#0-0-C-1] org.apache.kafka.clients.NetworkClient   : [Consumer clientId=consumer-order-service-1, groupId=order-service] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Node may not be available.
[...]
```

You can test the app by creating a simple order with this command:

```shell
curl -XPOST localhost:8080/api/orders -H 'Content-type: application/json' \
    -d '{"customerId": "lbroudoux", "productQuantities": [{"productName": "Millefeuille", "quantity": 1}], "totalPrice": 5.1}'
```

Result is not too bad: `{"productName":"Millefeuille","details":"Pastry Millefeuille is not available"}` but you'll see this error in logs:

```log
2026-06-01T10:50:41.173+02:00  INFO 49603 --- [nio-8080-exec-1] o.a.c.c.C.[Tomcat].[localhost].[/]       : Initializing Spring DispatcherServlet 'dispatcherServlet'
2026-06-01T10:50:41.173+02:00  INFO 49603 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Initializing Servlet 'dispatcherServlet'
2026-06-01T10:50:41.173+02:00  INFO 49603 --- [nio-8080-exec-1] o.s.web.servlet.DispatcherServlet        : Completed initialization in 0 ms
2026-06-01T10:50:41.218+02:00 ERROR 49603 --- [nio-8080-exec-1] org.acme.order.service.OrderService      : Got exception from Pastry client: I/O error on GET request for "http://localhost:8082/pastries/Millefeuille": null
```

> [!Important]
> To run the application locally, we need to have a Kafka broker up and running + the other dependencies corresponding to our Pastry API provider and reviewing system.

Instead of installing these services on our local machine, or using Docker to run these services manually,
we will use [Spring Boot support for Testcontainers at Development Time](https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#features.testing.testcontainers.at-development-time) to provision these services automatically.

> [!NOTE]
> Before Spring Boot 3.1.0, Testcontainers libraries are mainly used for testing.
Spring Boot 3.1.0 introduced out-of-the-box support for Testcontainers which not only simplified testing,
but we can use Testcontainers for local development as well.
>
> To learn more, please read [Spring Boot Application Testing and Development with Testcontainers](https://www.atomicjar.com/2023/05/spring-boot-3-1-0-testcontainers-for-testing-and-local-development/)

In order to see what's needed to run this, you may check the `pom.xml` file and uncomment the block between lines 61 and 68 to enable 
the `microcks-testcontainers` depedency like below:

```xml
      <dependency>
			<groupId>io.github.microcks</groupId>
			<artifactId>microcks-testcontainers</artifactId>
			<version>0.4.4</version>
			<scope>test</scope>
		</dependency>
```


## Create ContainersConfiguration class under src/test/java/org/acme/order

In order to specify the dependant services we need, we use a specific Spring `Configuration` class located into `/src/test/java`.

Let's create `ContainersConfiguration` class under `src/test/java/org/acme/order` to configure the required containers.

```java
package org.acme.order;

import io.github.microcks.testcontainers.MicrocksContainersEnsemble;
import io.github.microcks.testcontainers.connection.KafkaConnection;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class ContainersConfiguration {

   private static Network network = Network.newNetwork();
   
   @Bean
   @ServiceConnection
   KafkaContainer kafkaContainer() {
      KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener(() -> "kafka:19092");
      return kafkaContainer;
   }

   @Bean
   MicrocksContainersEnsemble microcksEnsemble(KafkaContainer kafkaContainer) {
      MicrocksContainersEnsemble ensemble = new MicrocksContainersEnsemble(network, "quay.io/microcks/microcks-uber:1.14.0-native")
            .withPostman()             // We need this to do contract-testing with Postman collection
            .withAsyncFeature()        // We need this for async mocking and contract-testing
            .withAccessToHost(true)    // We need this to access our webapp while it runs
            .withKafkaConnection(new KafkaConnection("kafka:19092"))   // We need this to connect to Kafka
            .withMainArtifacts("order-service-openapi.yaml", "order-events-asyncapi.yaml", "third-parties/apipastries-openapi.yaml")
            .withSecondaryArtifacts("order-service-postman-collection.json", "third-parties/apipastries-postman-collection.json")
            .withAsyncDependsOn(kafkaContainer);   // We need this to be sure Kafka will be up before Microcks async minion

      return ensemble;
   }

   @Bean
   public DynamicPropertyRegistrar endpointsProperties(MicrocksContainersEnsemble ensemble) {
      // We need to replace the default endpoints with those provided by Microcks.
      return (properties) -> {
         properties.add("application.pastries-base-url", () -> ensemble.getMicrocksContainer()
               .getRestMockEndpoint("API Pastries", "0.0.1"));
         properties.add("application.order-events-reviewed-topic", () -> ensemble.getAsyncMinionContainer()
               .getKafkaMockTopic("Order Events API", "0.1.0", "PUBLISH orders-reviewed"));
      };
   }
}
```

Let's understand what this configuration class does:

* `@TestConfiguration` annotation indicates that this configuration class defines the beans that can be used for Spring Boot tests.
* Spring Boot provides `ServiceConnection` support `KafkaConnectionDetails` out-of-the-box.
  So, we configured `KafkaContainer` as beans with `@ServiceConnection` annotation.
  This configuration will automatically start these containers and register the **Kafka** connection properties automatically.
* We also configure a `MicrocksContainersEnsemble` that will be responsible for providing mocks for our 3rd party systems.
  As REST Client URL properties are not standard ones, Microcks does not contribute any `ServiceConnection`. 
* Instead, we have the ability to use the `DynamicPropertyRegistrar` to wire our application properties corresponding to REST Client URL 
  and Kafka Topic name. This way our application is using the endpoints that are provided by Microcks.

And that's it! 🎉 You don't need to download and install extra-things, or clone other repositories and figure out how to start your dependant services. 

## Create TestOrderServiceApplication class under src/test/java/org/acme/order

Next, let's create a `TestOrderServiceApplication` class under `src/test/java` to start the application with the containers configuration.

```java
package org.acme.order;

import org.springframework.boot.SpringApplication;

/**
 * A Test instance of the OrderServiceApplication.
 */
class TestOrderServiceApplication {

   public static void main(String[] args) {
      SpringApplication.from(OrderServiceApplication::main)
            .with(ContainersConfiguration.class)
            .run(args);
   }
}
```

Run the `TestOrderServiceApplication` from our IDE or using the `./mvnw spring-boot:test-run` command and verify that the application starts successfully. 🙌

You should see the container startups messages into the logs:

```shell
[...]
11:01:59.469 [restartedMain] INFO  org.testcontainers.DockerClientFactory - Checking the system...
11:01:59.469 [restartedMain] INFO  org.testcontainers.DockerClientFactory - ✔︎ Docker server version should be at least 1.6.0
11:01:59.471 [restartedMain] INFO  tc.confluentinc/cp-kafka:7.5.0 - Creating container for image: confluentinc/cp-kafka:7.5.0
11:01:59.517 [restartedMain] INFO  tc.confluentinc/cp-kafka:7.5.0 - Container confluentinc/cp-kafka:7.5.0 is starting: efa615494f9db8d0a4c68515981b74213d4871bd68c26b806adbf1a0cd605c7e
11:02:01.469 [restartedMain] INFO  tc.confluentinc/cp-kafka:7.5.0 - Container confluentinc/cp-kafka:7.5.0 started in PT1.998526S
11:02:01.470 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber:1.14.0-native - Creating container for image: quay.io/microcks/microcks-uber:1.14.0-native
11:02:01.541 [restartedMain] INFO  tc.testcontainers/sshd:1.2.0 - Creating container for image: testcontainers/sshd:1.2.0
11:02:01.568 [restartedMain] INFO  tc.testcontainers/sshd:1.2.0 - Container testcontainers/sshd:1.2.0 is starting: 159606381d42e5acba2aaea750c7d1a0428450daa7391f0dae4340c697a7ce9f
11:02:01.712 [restartedMain] INFO  tc.testcontainers/sshd:1.2.0 - Container testcontainers/sshd:1.2.0 started in PT0.171619S
11:02:01.786 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber:1.14.0-native - Container quay.io/microcks/microcks-uber:1.14.0-native is starting: 8bc3eca74c136c53d0339543db02b8cb8890e018f7e7d091111c25ab96ad0c25
11:02:02.196 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber:1.14.0-native - Container quay.io/microcks/microcks-uber:1.14.0-native started in PT0.726629S
11:02:02.232 [restartedMain] INFO  tc.quay.io/microcks/microcks-postman-runtime:latest - Creating container for image: quay.io/microcks/microcks-postman-runtime:latest
11:02:02.261 [restartedMain] INFO  tc.quay.io/microcks/microcks-postman-runtime:latest - Container quay.io/microcks/microcks-postman-runtime:latest is starting: 2b3d05e2790678b5ccfb288eba68d1f13f85a4cf0366237dd54aff556bf96a34
11:02:02.637 [restartedMain] INFO  tc.quay.io/microcks/microcks-postman-runtime:latest - Container quay.io/microcks/microcks-postman-runtime:latest started in PT0.405526S
11:02:02.641 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber-async-minion:1.14.0 - Creating container for image: quay.io/microcks/microcks-uber-async-minion:1.14.0
11:02:02.674 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber-async-minion:1.14.0 - Container quay.io/microcks/microcks-uber-async-minion:1.14.0 is starting: 911fb8a8f48443bacdbb4c1474afd6494cbdad6b9f3f5731fef50cb1a90460a4
11:02:03.914 [restartedMain] INFO  tc.quay.io/microcks/microcks-uber-async-minion:1.14.0 - Container quay.io/microcks/microcks-uber-async-minion:1.14.0 started in PT1.273097S
11:02:04.206 [restartedMain] INFO  org.springframework.boot.devtools.autoconfigure.OptionalLiveReloadServer - LiveReload server is running on port 35729
11:02:04.225 [restartedMain] INFO  org.apache.coyote.http11.Http11NioProtocol - Starting ProtocolHandler ["http-nio-8080"]
11:02:04.229 [restartedMain] INFO  org.springframework.boot.web.embedded.tomcat.TomcatWebServer - Tomcat started on port 8080 (http) with context path '/'
[...]
```

Now, you can invoke the APIs using CURL or Postman or any of your favourite HTTP Client tools.

## Create an order

```shell
curl -XPOST localhost:8080/api/orders -H 'Content-type: application/json' \
    -d '{"customerId": "lbroudoux", "productQuantities": [{"productName": "Millefeuille", "quantity": 1}], "totalPrice": 5.1}' -v
```

You should get a response similar to the following:

```shell
< HTTP/1.1 201 
< Content-Type: application/json
< Transfer-Encoding: chunked
< Date: Mon, 01 Jun 2026 09:04:35 GMT
< 
* Connection #0 to host localhost left intact
{"id":"745dee7b-bb30-4076-8ede-e526c3a61060","status":"CREATED","customerId":"lbroudoux","productQuantities":[{"productName":"Millefeuille","quantity":1}],"totalPrice":5.1}%   
```

Now test with something else, requesting for another Pastry:

```shell
curl -XPOST localhost:8080/api/orders -H 'Content-type: application/json' \
    -d '{"customerId": "lbroudoux", "productQuantities": [{"productName": "Eclair Chocolat", "quantity": 1}], "totalPrice": 4.1}' -v
```

This time you get another "exception" response:

```shell
< HTTP/1.1 422 
< Content-Type: application/json
< Transfer-Encoding: chunked
< Date: Mon, 01 Jun 2026 09:04:59 GMT
< 
* Connection #0 to host localhost left intact
{"productName":"Eclair Chocolat","details":"Pastry Eclair Chocolat is not available"}%
```

and this is because Microcks has created different simulations for the Pastry API 3rd party API based on API artifacts we loaded.
Check the `src/test/resources/third-parties/apipastries-openapi.yaml` and `src/test/resources/third-parties/apipastries-postman-collection.json` files to get details.

### 🎁 Bonus step - Explore Microcks UI and understand dispatchers

* Use `docker ps` command or Docker Desktop to retrieve the local port where Microcks is actually started and open it in your browser
* Review the mocked APIs:

  * How can you visually check that the **API Pastries - 0.0.1** mock is called?
  * How can you figure out that Microcks is sending back the correct response?

* Explore [Dispatcher & dispatching rules](https://microcks.io/documentation/explanations/dispatching/) to learn more.

### 
[Next](step-4-write-rest-tests.md)