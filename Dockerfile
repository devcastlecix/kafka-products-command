FROM eclipse-temurin:25-jre
WORKDIR /app
COPY target/products-command-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]