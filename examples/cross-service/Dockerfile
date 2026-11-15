# Build from the repository root:
#   docker build -f examples/cross-service/Dockerfile .
FROM golang:1.27 AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/cross-service
RUN CGO_ENABLED=0 go build -o /cross-service .

FROM gcr.io/distroless/static-debian12
COPY --from=build /cross-service /cross-service
ENTRYPOINT ["/cross-service"]
