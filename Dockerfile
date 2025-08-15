FROM ghcr.io/graalvm/native-image-community:21 AS build

ENV LANG=C.UTF-8

WORKDIR /app

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw

RUN ./mvnw -B -q -Pnative -DskipTests dependency:go-offline

COPY src src
COPY src src

RUN ./mvnw -B -q -Pnative -DskipTests clean package


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/target/app /app/app
RUN mkdir -p /sockets

ENTRYPOINT ["/app/app"]
