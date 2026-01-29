# Build stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o fakelake main.go

# Runtime stage
FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=builder /app/fakelake /fakelake
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/fakelake"]
