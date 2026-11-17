# TLS test image: the stock QuestDB image built from core/Dockerfile (target questdb), plus a throwaway
# certificate and a TLS server.conf, started through io.questdb.tls.TlsServerMain the way deployments
# start it (a command override with the JVM module flags, since the entrypoint injects those flags only
# for its default command).
#
# Build from the repository root (a full clone, not a git worktree):
#   docker build -f core/Dockerfile --target questdb -t questdb-base:local .
#   docker build --build-arg BASE_IMAGE=questdb-base:local -t questdb-tls core/docker-tls
ARG BASE_IMAGE=questdb-base:local
FROM ${BASE_IMAGE}

# The image runs QuestDB as uid 10001, which must be able to read the key. --chown also applies to the
# directories COPY creates; --chmod would too, and strip their execute bit.
COPY --chown=10001:10001 server.crt server.key /etc/questdb/tls/
COPY --chown=10001:10001 server.conf /var/lib/questdb/conf/server.conf

EXPOSE 9000 8812 9009

CMD ["-ea", \
  "-Dnoebug", \
  "-XX:+UseParallelGC", \
  "--sun-misc-unsafe-memory-access=allow", \
  "--enable-native-access=io.questdb", \
  "--add-opens=java.base/java.lang=io.questdb", \
  "--add-opens=java.base/java.lang.reflect=io.questdb", \
  "--add-opens=java.base/java.nio=io.questdb", \
  "--add-opens=java.base/java.time.zone=io.questdb", \
  "--add-exports=java.base/jdk.internal.vm=io.questdb", \
  "-m", "io.questdb/io.questdb.tls.TlsServerMain", \
  "-d", "/var/lib/questdb"]
