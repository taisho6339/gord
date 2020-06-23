FROM golang:1.14.2 as gord-build

WORKDIR /go/src/app
COPY go.mod /go/src/app
COPY go.sum /go/src/app
RUN go mod download

COPY . /go/src/app
RUN go build -o /go/bin/app

FROM gcr.io/distroless/base

COPY --from=gord-build /go/bin/app /
EXPOSE 8080
CMD ["/app"]