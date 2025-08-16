
FROM ubuntu:22.04 AS builder
# Install all required dependencies including PostgreSQL dev libraries, OpenSSL
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        cmake \
        git \
        libboost-system-dev \
        libboost-thread-dev \
        libboost-random-dev \
        libwebsocketpp-dev \
        nlohmann-json3-dev \
        libpq-dev \
        libssl-dev \
        wget \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
# Configure and build the project
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --config Release

FROM ubuntu:22.04
# Install runtime dependencies including PostgreSQL client library, OpenSSL
RUN apt-get update && \
    apt-get install -y \
        libboost-system1.74.0 \
        libboost-thread1.74.0 \
        libpq5 \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/build/ThePlusTVServer .
# Set default CORS allowed origins
ENV ALLOWED_ORIGINS="https://the-super-sweet-two.vercel.app,http://localhost:3000"
EXPOSE 8080
CMD ["./ThePlusTVServer"]