FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /openrig-metrics .

FROM alpine:3.20
COPY --from=build /openrig-metrics /usr/local/bin/openrig-metrics
EXPOSE 8090
ENTRYPOINT ["openrig-metrics"]
