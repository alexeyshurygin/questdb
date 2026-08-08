FROM eclipse-temurin:25-jre

WORKDIR /app

COPY questdb.jar /app/questdb.jar
COPY server.crt /app/tls/server.crt
COPY server.key /app/tls/server.key
COPY server.conf /app/server.conf

RUN mkdir -p /var/lib/questdb/conf && \
    cp /app/server.conf /var/lib/questdb/conf/server.conf

EXPOSE 9000 8812 9009

ENTRYPOINT ["java", \
  "-ea", \
  "-XX:+UseParallelGC", \
  "--sun-misc-unsafe-memory-access=allow", \
  "--enable-native-access=io.questdb", \
  "--add-opens=java.base/java.lang=io.questdb", \
  "--add-opens=java.base/java.lang.reflect=io.questdb", \
  "--add-opens=java.base/java.nio=io.questdb", \
  "--add-opens=java.base/java.time.zone=io.questdb", \
  "--add-exports=java.base/jdk.internal.vm=io.questdb", \
  "-p", "/app/questdb.jar", \
  "-m", "io.questdb/io.questdb.tls.TlsServerMain", \
  "-d", "/var/lib/questdb"]
