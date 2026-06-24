FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/control-plane ./cmd/control-plane
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -o /out/infractl ./cmd/infractl

FROM alpine:3.20
COPY --from=build /out/control-plane /usr/local/bin/control-plane
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/infractl /usr/local/bin/infractl
ENTRYPOINT ["/usr/local/bin/control-plane"]
