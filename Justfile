DOCKER_CLI := env("DOCKER_CLI", "docker")
IMAGE_NAME := env("IMAGE_NAME", "rustfs:v1.0.0")
DOCKERFILE_SOURCE := env("DOCKERFILE_SOURCE", "Dockerfile.source")
DOCKERFILE_PRODUCTION := env("DOCKERFILE_PRODUCTION", "Dockerfile")
CONTAINER_NAME := env("CONTAINER_NAME", "rustfs-dev")

[group("📒 Help")]
[private]
default:
    @just --list --list-heading $'🦀 RustFS justfile manual page:\n'

[doc("show help")]
[group("📒 Help")]
help: default

[doc("run `cargo fmt` to format codes")]
[group("👆 Code Quality")]
fmt:
    @echo "🔧 Formatting code..."
    cargo fmt --all

[doc("run `cargo fmt` in check mode")]
[group("👆 Code Quality")]
fmt-check:
    @echo "📝 Checking code formatting..."
    cargo fmt --all --check

[doc("run `cargo clippy`")]
[group("👆 Code Quality")]
clippy:
    @echo "🔍 Running clippy checks..."
    cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings

[doc("run `cargo check`")]
[group("👆 Code Quality")]
check:
    @echo "🔨 Running compilation check..."
    cargo check --all-targets

[doc("run `cargo test`")]
[group("👆 Code Quality")]
test:
    @echo "🧪 Running tests..."
    cargo nextest run --all --exclude e2e_test
    cargo test --all --doc

[doc("run `fmt` `clippy` `check` `test` at once")]
[group("👆 Code Quality")]
pre-commit: fmt clippy check test
    @echo "✅ All pre-commit checks passed!"

[group("🤔 Git")]
setup-hooks:
    @echo "🔧 Setting up git hooks..."
    chmod +x .git/hooks/pre-commit
    @echo "✅ Git hooks setup complete!"

[doc("use `release` mode for building")]
[group("🔨 Build")]
build:
    @echo "🔨 Building RustFS using build-rustfs.sh script..."
    ./build-rustfs.sh

[doc("use `debug` mode for building")]
[group("🔨 Build")]
build-dev:
    @echo "🔨 Building RustFS in development mode..."
    ./build-rustfs.sh --dev

[group("🔨 Build")]
[private]
build-target target:
    @echo "🔨 Building rustfs for {{ target }}..."
    @echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
    ./build-rustfs.sh --platform {{ target }}

[doc("use `x86_64-unknown-linux-musl` target for building")]
[group("🔨 Build")]
build-musl: (build-target "x86_64-unknown-linux-musl")

[doc("use `x86_64-unknown-linux-gnu` target for building")]
[group("🔨 Build")]
build-gnu: (build-target "x86_64-unknown-linux-gnu")

[doc("use `aarch64-unknown-linux-musl` target for building")]
[group("🔨 Build")]
build-musl-arm64: (build-target "aarch64-unknown-linux-musl")

[doc("use `aarch64-unknown-linux-gnu` target for building")]
[group("🔨 Build")]
build-gnu-arm64: (build-target "aarch64-unknown-linux-gnu")

[doc("build and deploy to server")]
[group("🔨 Build")]
deploy-dev ip: build-musl
    @echo "🚀 Deploying to dev server: {{ ip }}"
    ./scripts/dev_deploy.sh {{ ip }}

[group("🔨 Build")]
[private]
build-cross-all-pre:
    @echo "🔧 Building all target architectures..."
    @echo "💡 On macOS/Windows, use 'make docker-dev' for reliable multi-arch builds"
    @echo "🔨 Generating protobuf code..."
    -cargo run --bin gproto

[doc("build all targets at once")]
[group("🔨 Build")]
build-cross-all: build-cross-all-pre && build-gnu build-gnu-arm64 build-musl build-musl-arm64

# ========================================================================================
# Docker Multi-Architecture Builds (Primary Methods)
# ========================================================================================

[doc("build an image and run it")]
[group("🐳 Build Image")]
build-docker os="rockylinux9.3" cli=(DOCKER_CLI) dockerfile=(DOCKERFILE_SOURCE):
    #!/usr/bin/env bash
    SOURCE_BUILD_IMAGE_NAME="rustfs/rustfs-{{ os }}:v1"
    SOURCE_BUILD_CONTAINER_NAME="rustfs-{{ os }}-build"
    BUILD_CMD="/root/.cargo/bin/cargo build --release --bin rustfs --target-dir /root/s3-rustfs/target/{{ os }}"
    echo "🐳 Building RustFS using Docker ({{ os }})..."
    {{ cli }} buildx build -t $SOURCE_BUILD_IMAGE_NAME -f {{ dockerfile }} .
    {{ cli }} run --rm --name $SOURCE_BUILD_CONTAINER_NAME -v $(pwd):/root/s3-rustfs -it $SOURCE_BUILD_IMAGE_NAME $BUILD_CMD

[doc("build an image")]
[group("🐳 Build Image")]
docker-buildx:
    @echo "🏗️ Building multi-architecture production Docker images with buildx..."
    ./docker-buildx.sh

[doc("build an image and push it")]
[group("🐳 Build Image")]
docker-buildx-push:
    @echo "🚀 Building and pushing multi-architecture production Docker images with buildx..."
    ./docker-buildx.sh --push

[doc("build an image with a version")]
[group("🐳 Build Image")]
docker-buildx-version version:
    @echo "🏗️ Building multi-architecture production Docker images (version: {{ version }}..."
    ./docker-buildx.sh --release {{ version }}

[doc("build an image with a version and push it")]
[group("🐳 Build Image")]
docker-buildx-push-version version:
    @echo "🚀 Building and pushing multi-architecture production Docker images (version: {{ version }}..."
    ./docker-buildx.sh --release {{ version }} --push

[doc("build an image with a version and push it to registry")]
[group("🐳 Build Image")]
docker-dev-push registry cli=(DOCKER_CLI) source=(DOCKERFILE_SOURCE):
    @echo "🚀 Building and pushing multi-architecture development Docker images..."
    @echo "💡 push to registry: {{ registry }}"
    {{ cli }} buildx build \
    	--platform linux/amd64,linux/arm64 \
    	--file {{ source }} \
    	--tag {{ registry }}/rustfs:source-latest \
    	--tag {{ registry }}/rustfs:dev-latest \
    	--push \
    	.

# Local production builds using direct buildx (alternative to docker-buildx.sh)

[group("🐳 Build Image")]
docker-buildx-production-local cli=(DOCKER_CLI) source=(DOCKERFILE_PRODUCTION):
    @echo "🏗️ Building single-architecture production Docker image locally..."
    @echo "💡 Alternative to docker-buildx.sh for local testing"
    {{ cli }} buildx build \
    	--file {{ source }} \
    	--tag rustfs:production-latest \
    	--tag rustfs:latest \
    	--load \
    	--build-arg RELEASE=latest \
    	.

# Development/Source builds using direct buildx commands

[group("🐳 Build Image")]
docker-dev cli=(DOCKER_CLI) source=(DOCKERFILE_SOURCE):
    @echo "🏗️ Building multi-architecture development Docker images with buildx..."
    @echo "💡 This builds from source code and is intended for local development and testing"
    @echo "⚠️  Multi-arch images cannot be loaded locally, use docker-dev-push to push to registry"
    {{ cli }} buildx build \
    	--platform linux/amd64,linux/arm64 \
    	--file {{ source }} \
    	--tag rustfs:source-latest \
    	--tag rustfs:dev-latest \
    	.

[group("🐳 Build Image")]
docker-dev-local cli=(DOCKER_CLI) source=(DOCKERFILE_SOURCE):
    @echo "🏗️ Building single-architecture development Docker image for local use..."
    @echo "💡 This builds from source code for the current platform and loads locally"
    {{ cli }} buildx build \
    	--file {{ source }} \
    	--tag rustfs:source-latest \
    	--tag rustfs:dev-latest \
    	--load \
    	.

# ========================================================================================
# Single Architecture Docker Builds (Traditional)
# ========================================================================================

[group("🐳 Build Image")]
docker-build-production cli=(DOCKER_CLI) source=(DOCKERFILE_PRODUCTION):
    @echo "🏗️ Building single-architecture production Docker image..."
    @echo "💡 Consider using 'make docker-buildx-production-local' for multi-arch support"
    {{ cli }} build -f {{ source }} -t rustfs:latest .

[group("🐳 Build Image")]
docker-build-source cli=(DOCKER_CLI) source=(DOCKERFILE_SOURCE):
    @echo "🏗️ Building single-architecture source Docker image..."
    @echo "💡 Consider using 'make docker-dev-local' for multi-arch support"
    {{ cli }} build -f {{ source }} -t rustfs:source .

# ========================================================================================
# Development Environment
# ========================================================================================

[group("🏃 Running")]
dev-env-start cli=(DOCKER_CLI) source=(DOCKERFILE_SOURCE) container=(CONTAINER_NAME):
    @echo "🚀 Starting development environment..."
    {{ cli }} buildx build \
    	--file {{ source }} \
    	--tag rustfs:dev \
    	--load \
    	.
    -{{ cli }} stop {{ container }} 2>/dev/null
    -{{ cli }} rm {{ container }} 2>/dev/null
    {{ cli }} run -d --name {{ container }} \
    	-p 9010:9010 -p 9000:9000 \
    	-v {{ invocation_directory() }}:/workspace \
    	-it rustfs:dev

[group("🏃 Running")]
dev-env-stop cli=(DOCKER_CLI) container=(CONTAINER_NAME):
    @echo "🛑 Stopping development environment..."
    -{{ cli }} stop {{ container }} 2>/dev/null
    -{{ cli }}  rm {{ container }} 2>/dev/null

[group("🏃 Running")]
dev-env-restart: dev-env-stop dev-env-start

[group("👍 E2E")]
e2e-server:
    sh scripts/run.sh

[group("👍 E2E")]
probe-e2e:
    sh scripts/probe.sh

[doc("inspect one image")]
[group("🚚 Other")]
docker-inspect-multiarch image cli=(DOCKER_CLI):
    @echo "🔍 Inspecting multi-architecture image: {{ image }}"
    {{ cli }} buildx imagetools inspect {{ image }}


# ========================================================================================
# io_uring convenience recipes (Linux-only acceleration)
# ========================================================================================

[doc("build with io_uring feature (Linux)")]
[group("🔨 Build")]
build-uring:
        @echo "⚡ Building RustFS with io_uring enabled..."
        if [ "$(uname -s)" != "Linux" ]; then \
            echo "⚠️  io_uring 仅在 Linux 可用，回退为常规构建"; \
            ./build-rustfs.sh; \
        else \
            ./build-rustfs.sh --features uring-io; \
        fi

[doc("build (dev) with io_uring feature (Linux)")]
[group("🔨 Build")]
build-dev-uring:
        @echo "⚡ Building RustFS (dev) with io_uring enabled..."
        if [ "$(uname -s)" != "Linux" ]; then \
            echo "⚠️  io_uring 仅在 Linux 可用，回退为常规开发构建"; \
            ./build-rustfs.sh --dev; \
        else \
            ./build-rustfs.sh --dev --features uring-io; \
        fi

[doc("run tests with io_uring feature (Linux)")]
[group("👆 Code Quality")]
test-uring:
        @echo "🧪 Running tests with io_uring feature..."
        if [ "$(uname -s)" != "Linux" ]; then \
            echo "⚠️  io_uring 仅在 Linux 可用，回退为常规测试"; \
            cargo nextest run --all --exclude e2e_test; \
            cargo test --all --doc; \
        else \
            cargo nextest run --all --features uring-io --exclude e2e_test || true; \
            cargo test --all --features uring-io --doc || true; \
        fi
