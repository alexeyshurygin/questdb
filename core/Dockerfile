FROM debian:bookworm AS builder

ARG tag_name
# Oracle GraalVM for JDK 25, licensed under the GraalVM Free Terms and
# Conditions (GFTC). 25.0.5.1 is the November 2026 Critical Security Patch
# Update, built on Oracle JDK 25.0.5.1+4; it fixes CVE-2026-114938, which
# affects JDK 25.0.5.0.3 and earlier, including GraalVM CE 25.0.2.
# Oracle serves these archives from the GraalVM Download Service (GDS), the
# same source SDKMAN uses for 25.0.5.1-graal. The artifact ids and SHA-256
# values below come from the GDS artifact listing for release 25.0.5.1; a
# version bump needs all four values replaced together.
ARG GRAAL_JDK_VERSION=25.0.5.1
ARG GRAAL_GDS_ARTIFACT_AMD64=3463261B18A64C2A9362B37B7B3CE4A2
ARG GRAAL_SHA256_AMD64=b3505b8e8f73726885044820f7de593c7b0273c691286655cdb7ece2a23ad75a
ARG GRAAL_GDS_ARTIFACT_ARM64=9ECF73F3B5474400B4A1441B688B1438
ARG GRAAL_SHA256_ARM64=a851787f392e43da695531cd3fc6b889cfa7d1b9d71503c9e6adbfcdafd0c83d
ARG TARGETARCH

ENV GOSU_VERSION=1.19-go1.25.14
ENV DEBIAN_FRONTEND=noninteractive

# 1) Base tools + Maven
# `git` is required by buildnumber-maven-plugin (core/pom.xml) at build time
# to embed the HEAD revision; keep it installed even though the sources now
# come from the build context rather than a `git clone`.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
       git curl wget gnupg2 ca-certificates lsb-release software-properties-common unzip maven \
    && rm -rf /var/lib/apt/lists/*

# 2) Download Oracle GraalVM, verify the pinned SHA256 and install it.
# The checksum is pinned in this file rather than fetched next to the archive,
# so a replaced or tampered download fails the build instead of passing.
RUN set -eux; \
    : "${TARGETARCH:=amd64}"; \
    case "$TARGETARCH" in \
      amd64) ARTIFACT="${GRAAL_GDS_ARTIFACT_AMD64}"; HASH="${GRAAL_SHA256_AMD64}" ;; \
      arm64) ARTIFACT="${GRAAL_GDS_ARTIFACT_ARM64}"; HASH="${GRAAL_SHA256_ARM64}" ;; \
      *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    echo "Building for TARGETARCH=${TARGETARCH}, using Oracle GraalVM ${GRAAL_JDK_VERSION} (GDS artifact ${ARTIFACT})"; \
    FILENAME="graalvm-jdk-${GRAAL_JDK_VERSION}.tar.gz"; \
    mkdir -p /tmp/graalvm; \
    cd /tmp/graalvm; \
    curl -fL -o "${FILENAME}" "https://gds.oracle.com/api/20220101/artifacts/${ARTIFACT}/content"; \
    echo "${HASH}  ${FILENAME}" | sha256sum -c -; \
    INSTALL_DIR="/usr/lib/jvm/graalvm-jdk-${GRAAL_JDK_VERSION}"; \
    mkdir -p "${INSTALL_DIR}"; \
    tar -xzf "${FILENAME}" -C "${INSTALL_DIR}" --strip-components=1; \
    cd /; \
    rm -rf /tmp/graalvm; \
    "${INSTALL_DIR}/bin/java" -version

ENV JAVA_HOME=/usr/lib/jvm/graalvm-jdk-${GRAAL_JDK_VERSION}
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN echo tag_name ${tag_name:-master}

WORKDIR /build/questdb

COPY . .

# Auto-detect how the java-questdb-client dependency must be resolved.
# If core/pom.xml references a -SNAPSHOT client version, that artifact is not
# published to Maven Central, so we build it from the bundled java-questdb-client
# submodule by enabling the `local-client` Maven profile. For a released client
# version we resolve it from Maven Central (no extra profile).
# NOTE: for the SNAPSHOT path the submodule must be present in the build context
# (the docker-release pipeline checks it out via `submodules: true`) and its
# version must match questdb.client.version.
RUN set -eux; \
    CLIENT_VERSION="$(sed -n 's/.*<questdb.client.version>\(.*\)<\/questdb.client.version>.*/\1/p' core/pom.xml | head -1)"; \
    echo "questdb.client.version=${CLIENT_VERSION}"; \
    CLIENT_PROFILE=""; \
    case "${CLIENT_VERSION}" in \
      *-SNAPSHOT) echo "SNAPSHOT client detected, activating local-client profile"; CLIENT_PROFILE=",local-client" ;; \
      *) echo "Release client detected, resolving from Maven Central" ;; \
    esac; \
    mvn clean package -Djdk.lang.Process.launchMechanism=vfork -Dmaven.resolver.transport=wagon -Dmaven.wagon.httpconnectionManager.ttlSeconds=30 -DskipTests -P "build-web-console,build-binaries${CLIENT_PROFILE}"

WORKDIR /build/questdb/core/target

RUN tar xvfz questdb-*-rt-*.tar.gz

RUN rm questdb-*-rt-*.tar.gz

RUN dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
    wget -O "gosu-$dpkgArch" "https://github.com/questdb/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" && \
    wget -O SHA256SUMS "https://github.com/questdb/gosu/releases/download/$GOSU_VERSION/SHA256SUMS" && \
    grep "gosu-$dpkgArch\$" SHA256SUMS | sha256sum -c - && \
    rm SHA256SUMS && \
    mv "gosu-$dpkgArch" gosu && \
    chmod +x gosu && \
    ./gosu --version && \
    ./gosu nobody true

##############################################################################
######################### Build for RedHat OpenShift #########################
##############################################################################

FROM registry.access.redhat.com/ubi10/ubi-minimal AS rhel
ARG tag_name

WORKDIR /app

COPY --from=builder /build/questdb/core/target/questdb-*-rt-* .

COPY core/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Add license information
RUN mkdir /licenses
COPY core/LICENSE.txt /licenses

LABEL name=QuestDB \
    vendor=QuestDB \
    summary="Provides the latest release of QuestDB" \
    description="QuestDB is an open source time-series database for fast ingest and SQL queries" \
    version=${tag_name:-master} \
    release=${tag_name:-master} \
    maintainer=QuestDB \
    url="https://github.com/questdb/questdb"

# Create questdb user and group
RUN microdnf install -y shadow-utils util-linux grep && \
    groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    microdnf remove -y shadow-utils && \
    microdnf clean all && \
    mkdir -p /var/lib/questdb && \
    chown -R questdb:questdb /var/lib/questdb

USER questdb

WORKDIR /var/lib/questdb

# Make port 9000 available to the world outside this container
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

ENV DO_CHOWN="false"
ENTRYPOINT ["/docker-entrypoint.sh"]

##############################################################################
########################## General-use Docker build ##########################
##############################################################################

FROM quay.io/fedora/fedora-minimal:44 AS questdb
ARG tag_name

WORKDIR /app

COPY --from=builder /build/questdb/core/target/questdb-*-rt-* .
COPY --from=builder /build/questdb/core/target/gosu /usr/local/bin/gosu

COPY core/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create questdb user and group
RUN microdnf install -y shadow-utils util-linux grep && \
    groupadd -g 10001 questdb && \
    useradd -u 10001 -g 10001 -d /var/lib/questdb -M -s /sbin/nologin questdb && \
    microdnf remove -y shadow-utils && \
    microdnf clean all && \
    mkdir -p /var/lib/questdb && \
    chown -R questdb:questdb /var/lib/questdb

WORKDIR /var/lib/questdb

# Make port 9000 available to the world outside this container
EXPOSE 9000/tcp
EXPOSE 8812/tcp
EXPOSE 9009/tcp

#
# Run QuestDB when the container launches
#
# 'conf/log.conf' is a placeholder, it does not exist out of box
# which make logger use default configuration. However, when user configures
# a volume with something like:
#
# docker run -v "$(pwd):/var/lib/questdb/"  questdb/questdb
#
# then one can create 'log.conf' in the 'conf' dir and override logger fully
#
ENTRYPOINT ["/docker-entrypoint.sh"]
