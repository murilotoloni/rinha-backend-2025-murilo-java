FROM ghcr.io/graalvm/native-image-community:21 AS build
WORKDIR /app

ARG NPROC=4
ENV NPROC=${NPROC}
ENV LANG=C.UTF-8

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
RUN ./mvnw -T 1C -B -q -Pnative -DskipTests dependency:go-offline

COPY src src
RUN ./mvnw -T 1C -B -q -Pnative -DskipTests \
    -Dgpg.skip=true -Dmaven.javadoc.skip=true clean package

# runtime
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/target/app /app/app


ENTRYPOINT ["/app/app"]
