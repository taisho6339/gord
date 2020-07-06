FROM golang:1.14.2 as gord-build

WORKDIR /go/src/app
COPY go.mod /go/src/app
COPY go.sum /go/src/app
RUN go mod download

COPY . /go/src/app
RUN make build

FROM gcr.io/distroless/base

COPY --from=gord-build /go/src/app/gordctl /
ENTRYPOINT ["/gordctl"]
CMD ["-l", "", "-n", ""]