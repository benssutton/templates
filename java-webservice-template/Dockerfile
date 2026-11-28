# syntax=docker/dockerfile:1
FROM eclipse-temurin:27.0.1.1_4-jdk@sha256:a235abaaf912abaa4e806f6977009dc999ef04f1a75c9156626780884e807ee1 AS build
WORKDIR /build
COPY .mvn ./.mvn
COPY mvnw pom.xml ./
# The Maven repository is a BuildKit cache mount, so dependencies download once and a
# failed download fails the build here instead of being masked.
RUN --mount=type=cache,target=/root/.m2 chmod +x mvnw && ./mvnw -q -B dependency:resolve
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -DskipTests package

# Demo publisher for docker compose (`target: publisher`). It lives in the test sources
# so it never ships in the application jar; it runs from compiled test classes.
FROM build AS publisher
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -DskipTests test-compile dependency:build-classpath \
      -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=test
USER 10001
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow"
CMD ["sh", "-c", "exec java -cp target/classes:target/test-classes:$(cat target/classpath.txt) com.example.template.publisher.ExampleSolacePublisher"]

FROM eclipse-temurin:27.0.1.1_4-jre@sha256:9723bc085e2a7bd142b78c9a293427941c309962276d8ef28053a61f73238930
WORKDIR /app
# curl backs the HEALTHCHECK; it is not in the base JRE image.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
# target/app.jar is the runnable fat jar (finalName=app); original-app.jar is the thin jar.
COPY --from=build /build/target/app.jar app.jar
ENV MICRONAUT_ENVIRONMENTS=docker
# Arrow needs direct nio access. Heap is sized from the container memory limit, and an
# OutOfMemoryError exits so the orchestrator restarts the container.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
# The TLS keystore is not baked in: mount one at /app/certs/keystore.p12 (compose
# generates a self-signed dev keystore into a volume).
USER 10001
EXPOSE 8443
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=6 \
  CMD curl -fsk https://localhost:8443/health/live || exit 1
CMD ["java", "-jar", "app.jar"]
