# --- Build stage: compile a static binary ---------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src

# Download dependencies first, so this layer is cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/throttlex ./cmd/server

# --- Runtime stage: no shell, no package manager, non-root user -----------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/throttlex /throttlex
USER nonroot:nonroot
EXPOSE 8080
# The image has no curl, so the binary probes its own /health endpoint.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/throttlex", "-healthcheck"]
ENTRYPOINT ["/throttlex"]
