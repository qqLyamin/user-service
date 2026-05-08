FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates cmake libpq-dev ninja-build \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DUSER_SERVICE_USE_LIBPQ=ON -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc" \
    && cmake --build build --target user-service

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/user-service /app/user-service
COPY migrations /app/migrations

EXPOSE 8812
ENTRYPOINT ["/app/user-service"]
