FROM node:24-bookworm-slim@sha256:ba849c60be29959425b8734d57b8b4b7d56f98edd9504c9af091d5281095a71e

ARG TARGETARCH
ARG GO_VERSION=1.27.0
ARG GO_SHA256_AMD64=675c26c449cbb18fc24b74650de1eabbae6e16f64326fd85a283fb3b58280685
ARG GO_SHA256_ARM64=51798d2c42d0e1c6ed7fd9f48728b4193abac9e8aad6dbac2fe96a81f5909bda

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl gh git jq openssh-client ripgrep ruby \
    && rm -rf /var/lib/apt/lists/* \
    && case "$TARGETARCH" in amd64) GO_SHA256="$GO_SHA256_AMD64" ;; arm64) GO_SHA256="$GO_SHA256_ARM64" ;; *) exit 1 ;; esac \
    && curl -fsSLo /tmp/go.tgz "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" \
    && echo "${GO_SHA256}  /tmp/go.tgz" | sha256sum -c - \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm /tmp/go.tgz \
    && ln -s /usr/local/go/bin/go /usr/local/bin/go \
    && ln -s /usr/local/go/bin/gofmt /usr/local/bin/gofmt \
    && npm install --global --ignore-scripts @earendil-works/pi-coding-agent@0.84.3 \
    && npm cache clean --force

COPY lib/backstage/errors.rb /opt/backstage/lib/backstage/errors.rb
COPY lib/backstage/domain/repository_authority.rb /opt/backstage/lib/backstage/domain/repository_authority.rb
COPY --chmod=0755 scripts/backstage-container-repository scripts/backstage-repository-authority scripts/backstage-agent-request /usr/local/bin/

ENV PATH="/usr/local/go/bin:${PATH}"
WORKDIR /workspace

LABEL org.opencontainers.image.title="Backstage worker" \
      org.opencontainers.image.description="Stable pi, git, gh, and Go worker image"

CMD ["bash"]
