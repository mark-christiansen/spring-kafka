FROM adoptopenjdk/openjdk11:alpine-slim

RUN apk update && apk add --no-cache ca-certificates curl wget

# setup jmx prometheus exporter
COPY jmx/jmx_prometheus_javaagent*.jar /jmx/jmx_exporter.jar
COPY jmx/config.yml /jmx/config.yml
RUN chgrp -R 0 /jmx && chmod -R g=u /jmx

COPY build/libs/producer-*.jar /app/app.jar

WORKDIR /app
EXPOSE 7072

ENTRYPOINT ["sh", "-c", "java -jar $JAVA_OPTS -javaagent:/jmx/jmx_exporter.jar=7072:/jmx/config.yml /app/app.jar"]