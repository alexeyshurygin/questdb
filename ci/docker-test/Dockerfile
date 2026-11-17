# syntax=docker/dockerfile:1.7
#
# Linux images for the local parallel test runner, ci/docker-test/run-tests.sh. The runner builds them from a
# `git archive` of the revision under test, so nothing outside that revision reaches the image.
#
#   java-tests  the compiled project and an offline Maven repository; each container runs one shard of test classes
#   rust-tests  the pinned Rust toolchain and the sources, for the crates' tests and lints

ARG JDK_IMAGE=eclipse-temurin:25-jdk-noble

FROM ${JDK_IMAGE} AS base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends maven git curl ca-certificates build-essential python3 nodejs \
    && rm -rf /var/lib/apt/lists/*
# Tests run as an unprivileged user: root ignores file permissions, and some tests rely on them.
RUN useradd --create-home --uid 10001 --shell /bin/bash qdb
USER qdb
WORKDIR /home/qdb/src

FROM base AS rust-toolchain
ENV RUSTUP_HOME=/home/qdb/.rustup \
    CARGO_HOME=/home/qdb/.cargo \
    PATH=/home/qdb/.cargo/bin:$PATH
COPY --chown=qdb:qdb core/rust/qdbr/rust-toolchain.toml /tmp/toolchain/rust-toolchain.toml
# Installs exactly what the crate's toolchain file asks for, so cargo has nothing left to download later.
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none \
    && cd /tmp/toolchain && rustup toolchain install \
    && toolchain="$(rustup show active-toolchain | cut -d' ' -f1)" \
    && rustup component add --toolchain "$toolchain" clippy rustfmt \
    && rustup default "$toolchain"

FROM rust-toolchain AS rust-lib
COPY --chown=qdb:qdb . .
# The cargo caches survive between builds, so an unchanged crate is not compiled again.
RUN --mount=type=cache,target=/home/qdb/.cargo/registry,uid=10001,gid=10001 \
    --mount=type=cache,target=/home/qdb/cargo-target,uid=10001,gid=10001 \
    cd core/rust/qdbr \
    && CARGO_INCREMENTAL=0 cargo build -j 4 --release --lib --target-dir /home/qdb/cargo-target \
    && mkdir -p /home/qdb/qdbr \
    && cp /home/qdb/cargo-target/release/libquestdbr.so /home/qdb/qdbr/

FROM rust-toolchain AS rust-tests
ARG PARQUET_TESTING_SHA=
COPY --chown=qdb:qdb . .
# parquet2's tests read apache/parquet-testing, a submodule that `git archive` leaves out.
RUN if [ -n "$PARQUET_TESTING_SHA" ]; then \
        dir=core/rust/parquet2/testing/parquet-testing \
        && rm -rf "$dir" && mkdir -p "$dir" && cd "$dir" \
        && git init -q && git remote add origin https://github.com/apache/parquet-testing \
        && git fetch -q --depth 1 origin "$PARQUET_TESTING_SHA" && git checkout -q FETCH_HEAD; \
    fi
COPY --chown=qdb:qdb ci/docker-test/run-rust.sh /home/qdb/run-rust.sh
ENTRYPOINT ["bash", "/home/qdb/run-rust.sh"]

FROM base AS java-build
COPY --chown=qdb:qdb . .
COPY --from=rust-lib --chown=qdb:qdb /home/qdb/qdbr /home/qdb/qdbr
# Maven's repository lives in a build cache and is copied into the image at the end, so the test containers run
# offline and a rebuild does not download everything again. The core jar is installed into it for the utils and
# compat tests, and each module's test runner is fetched by running one quick test class in it.
# A capped heap keeps a build from taking the memory that running test containers rely on.
ENV MAVEN_OPTS="-Xmx3g -XX:+UseParallelGC"
RUN --mount=type=cache,target=/home/qdb/.m2,uid=10001,gid=10001 \
    set -eux \
    && mvn -B -q -T 4 -Dmaven.resolver.transport=wagon -Dmaven.wagon.http.retryHandler.count=3 \
        -DskipNative -Dqdbr.prebuilt.dir=/home/qdb/qdbr -P build-web-console \
        test-compile \
    && mvn -B -q -pl core -DskipTests -DskipNative -Dqdbr.prebuilt.dir=/home/qdb/qdbr -P build-web-console \
        -Dmaven.javadoc.skip=true -Dmaven.source.skip=true install \
    && mvn -B -q -pl core,utils,compat org.apache.maven.plugins:maven-surefire-plugin:test \
        -Dtest=io.questdb.test.std.NumbersTest,io.questdb.cliutil.TxSerializerTest,io.questdb.compat.HttpClientCompatTest \
        -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -Dout=/home/qdb/src/ci/qlog.conf \
    && rm -rf core/target/surefire-reports utils/target/surefire-reports compat/target/surefire-reports \
    && mkdir -p /home/qdb/m2 && cp -a /home/qdb/.m2/repository /home/qdb/m2/repository

FROM base AS java-tests
COPY --from=java-build --chown=qdb:qdb /home/qdb/m2/repository /home/qdb/.m2/repository
COPY --from=java-build --chown=qdb:qdb /home/qdb/src /home/qdb/src
COPY --chown=qdb:qdb ci/docker-test/run-java.sh /home/qdb/run-java.sh
ENTRYPOINT ["bash", "/home/qdb/run-java.sh"]
