# syntax = docker/dockerfile:experimental

# git@github.com:phadej/docker-ghc.git
FROM docker-ghc:8.10.4-focal as builder

# A path we work in
WORKDIR /build

# cabal-install configuration
# - we'll be in better control of the build environment, than with default config.
COPY docker.cabal.config /build/cabal.config
ENV CABAL_CONFIG /build/cabal.config

# Update cabal-install database
RUN cabal update

# Install cabal-plan
# - we'll need it to find build artifacts
# - note: actual build tools ought to be specified in build-tool-depends field
RUN cabal install cabal-plan \
  --constraint='cabal-plan ^>=0.7' \
  --constraint='cabal-plan +exe' \
  --installdir=/usr/local/bin

COPY *.cabal /build/
RUN --mount=type=cache,target=dist-newstyle cabal build --only-dependencies

# Add rest of the files into build environment
# - remember to keep .dockerignore up to date
COPY . /build

# BUILD!!!
RUN --mount=type=cache,target=dist-newstyle cabal build exe:franzd \
  && mkdir -p /build/artifacts && cp $(cabal-plan list-bin franzd) /build/artifacts/

# Make a final binary a bit smaller
# RUN upx /build/artifacts/franzd

# DEPLOYMENT IMAGE
##############################################################################

FROM ubuntu:20.04
LABEL author="Fumiaki Kinoshita <fumiexcel@gmail.com>"

# Dependencies
# - no -dev stuff
# - cleanup apt stuff after installation
RUN apt-get -yq update && apt-get -yq --no-install-suggests --no-install-recommends install \
    ca-certificates \
    curl \
    libgmp10 \
    liblapack3 \
    liblzma5 \
    libpq5 \
    libssl1.1 \
    libyaml-0-2 \
    netbase \
    openssh-client \
    zlib1g \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Copy build artifact from a builder stage
COPY --from=builder /build/artifacts/franzd /app/franzd

# Set up a default command to run
ENTRYPOINT ["/app/franzd"]
