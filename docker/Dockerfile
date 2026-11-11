# The sandbox every attempt runs in: Go and Maelstrom's Go library for writing a node, a Java
# runtime and Maelstrom for testing it. Containers made from it run with no network, so
# everything a solution needs is downloaded here, at build time.
FROM golang:1.27

ARG MAELSTROM_VERSION=0.2.4
ARG MAELSTROM_SHA256=301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799
ARG MAELSTROM_GO_VERSION=v0.0.0-20260710121306-480a8197026d

# Maelstrom is a Clojure program: it needs a JVM, gnuplot for its latency graphs, and Graphviz,
# whose dot the transaction checker runs to draw what it found. Without either, the checker
# that needs it can't decide and the run comes out unknown.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless gnuplot-nox graphviz bzip2 \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/maelstrom.tar.bz2 \
      "https://github.com/jepsen-io/maelstrom/releases/download/v${MAELSTROM_VERSION}/maelstrom.tar.bz2" \
 && echo "${MAELSTROM_SHA256}  /tmp/maelstrom.tar.bz2" | sha256sum -c - \
 && tar -xjf /tmp/maelstrom.tar.bz2 -C /opt \
 && ln -s /opt/maelstrom/maelstrom /usr/local/bin/maelstrom \
 && rm /tmp/maelstrom.tar.bz2

# Maelstrom's Go library goes into the module cache, so a solution builds offline.
RUN mkdir /tmp/warm && cd /tmp/warm \
 && go mod init warm \
 && go mod download "github.com/jepsen-io/maelstrom/demo/go@${MAELSTROM_GO_VERSION}" \
 && rm -rf /tmp/warm \
 && chmod -R a+rX /go

# maelstrom-rename, which grading puts between Maelstrom and a node when a task renames its
# protocol. It uses only the standard library.
COPY rename /tmp/rename
RUN cd /tmp/rename && go build -o /usr/local/bin/maelstrom-rename . && rm -rf /tmp/rename

# At run time: only the cache, no proxy, no checksum database.
ENV GOPROXY=off GOSUMDB=off GOFLAGS=-mod=mod GOTOOLCHAIN=local

RUN useradd --create-home --uid 1000 agent \
 && mkdir /work && chown agent /work
USER agent
WORKDIR /work
