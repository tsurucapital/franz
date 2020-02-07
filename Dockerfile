# syntax = docker/dockerfile:experimental

FROM fumieval/ubuntu-ghc:18.04-8.8.2 as builder

WORKDIR /build

COPY docker.cabal.config /build/cabal.config
ENV CABAL_CONFIG /build/cabal.config

RUN cabal update

RUN cabal install cabal-plan \
  --constraint='cabal-plan ^>=0.6' \
  --constraint='cabal-plan +exe' \
  --installdir=/usr/local/bin

COPY *.cabal /build/
RUN --mount=type=cache,target=dist-newstyle cabal build --only-dependencies

COPY . /build

RUN --mount=type=cache,target=dist-newstyle cabal build exe:franzd \
  && mkdir -p /build/artifacts && cp $(cabal-plan list-bin franzd) /build/artifacts/

RUN upx /build/artifacts/franzd; done

FROM ubuntu:18.04

RUN apt-get -yq update && apt-get -yq --no-install-suggests --no-install-recommends install \
    ca-certificates \
    curl \
    libgmp10 \
    liblzma5 \
    libssl1.1 \
    libyaml-0-2 \
    netbase \
    zlib1g \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/artifacts/franzd /app/franzd

EXPOSE 1886
ENTRYPOINT ["./franzd"]
