# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/plantstream ./cmd/plantstream && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/plc-sim ./cmd/plc-sim

FROM gcr.io/distroless/static-debian12:nonroot AS plantstream
WORKDIR /app
COPY --from=build /out/plantstream /app/plantstream
COPY --from=build /src/examples /app/examples
USER nonroot:nonroot
EXPOSE 8080
ENV PLANTSTREAM_HTTP_ADDR=:8080 PLANTSTREAM_PLANT_FILE=/app/examples/plant.yaml
ENTRYPOINT ["/app/plantstream"]

FROM gcr.io/distroless/static-debian12:nonroot AS plc-sim
WORKDIR /app
COPY --from=build /out/plc-sim /app/plc-sim
USER nonroot:nonroot
EXPOSE 5020
ENTRYPOINT ["/app/plc-sim", "-addr", ":5020"]

# Default target is the edge node.
FROM plantstream
