FROM eclipse-temurin:21-jdk-jammy AS build
ARG SBT_VERSION=1.12.11
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
RUN curl -fL "https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch-${SBT_VERSION}.jar" -o /opt/sbt-launch.jar
COPY project/ project/
COPY build.sbt ./
COPY domain/ domain/
COPY service/ service/
COPY security/ security/
COPY db/ db/
COPY http/ http/
COPY app/ app/
RUN java -Dsbt.server.autostart=false -jar /opt/sbt-launch.jar "app/assembly"

FROM eclipse-temurin:21-jre-jammy
RUN useradd --system --uid 10001 --create-home app
WORKDIR /app
COPY --from=build /build/app/target/scala-3.9.0/p2.jar /app/p2.jar
USER 10001
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/p2.jar"]
