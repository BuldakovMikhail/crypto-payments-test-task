FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /payments ./cmd/payments

FROM alpine:3.22
RUN adduser -D -u 10001 app
USER app
COPY --from=build /payments /payments
EXPOSE 8080
ENTRYPOINT ["/payments"]
