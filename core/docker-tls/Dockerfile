FROM eclipse-temurin:17-jre

WORKDIR /app

COPY questdb.jar /app/questdb.jar
COPY server.crt /app/tls/server.crt
COPY server.key /app/tls/server.key
COPY server.conf /app/server.conf

RUN mkdir -p /var/lib/questdb/conf && \
    cp /app/server.conf /var/lib/questdb/conf/server.conf

EXPOSE 9000 8812 9009

ENTRYPOINT ["java", \
  "-p", "/app/questdb.jar", \
  "-m", "io.questdb/io.questdb.tls.TlsServerMain", \
  "-d", "/var/lib/questdb"]
