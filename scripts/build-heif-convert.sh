#!/usr/bin/env bash
#
# Build a statically-linked heif-convert binary for ARM64 Amazon Linux 2023.
# Uses Docker to compile inside the Lambda runtime environment.
#
# Output: lambdas/bin/heif-convert-arm64
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="$PROJECT_ROOT/lambdas/bin"

LIBHEIF_VERSION="v1.18.2"

mkdir -p "$OUTPUT_DIR"

echo "Building heif-convert ${LIBHEIF_VERSION} for ARM64 (Amazon Linux 2023)..."

docker run --rm --platform linux/arm64 \
  -v "$OUTPUT_DIR:/output" \
  public.ecr.aws/amazonlinux/amazonlinux:2023 \
  bash -c "
set -euo pipefail

# Install build dependencies
dnf install -y gcc gcc-c++ cmake3 make git \
  libde265-devel libjpeg-turbo-devel libpng-devel

# Clone canonical libheif repo
cd /tmp
git clone --depth 1 --branch ${LIBHEIF_VERSION} https://github.com/strukturag/libheif.git
cd libheif

# Build heif-convert with static linking
mkdir build && cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF \
  -DWITH_EXAMPLES=ON
make -j\$(nproc) heif-convert

# Copy output
cp examples/heif-convert /output/heif-convert-arm64
chmod +x /output/heif-convert-arm64
echo 'Build complete'
"

echo "Output: $OUTPUT_DIR/heif-convert-arm64"
file "$OUTPUT_DIR/heif-convert-arm64"
