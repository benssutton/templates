# syntax=docker/dockerfile:1
FROM eclipse-temurin:25-jdk AS build
WORKDIR /build
COPY .mvn ./.mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -q -B dependency:go-offline || true
COPY src ./src
RUN ./mvnw -q -B -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
COPY certs/generate-keystore.sh certs/generate-keystore.sh
# Self-signed dev keystore for HTTPS on 443 (keytool ships with the JRE image).
RUN bash certs/generate-keystore.sh certs/keystore.p12
ENV MICRONAUT_ENVIRONMENTS=docker
# Apache Arrow needs direct nio access on modern JDKs.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
EXPOSE 443
CMD ["java", "-jar", "app.jar"]
