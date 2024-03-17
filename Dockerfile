# Build the application
FROM golang:1.21

WORKDIR /go/src/github.com/voidshard/igor
COPY go.mod ./
COPY go.sum ./
COPY cmd cmd
COPY pkg pkg
COPY internal internal
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /igor ./cmd/igor/*.go

# Create a minimal image
FROM alpine

ARG USER=app
ARG GROUPNAME=$USER
ARG UID=12345
ARG GID=23456

RUN addgroup \
    --gid "$GID" \
    "$GROUPNAME" \
&&  adduser \
    --disabled-password \
    --gecos "" \
    --home "$(pwd)" \
    --ingroup "$GROUPNAME" \
    --no-create-home \
    --uid "$UID" \
    $USER

COPY --from=0 /igor /igor
COPY migrations migrations
RUN chown -R $USER:$GROUPNAME /igor migrations

USER $USER
ENTRYPOINT ["/igor"]
