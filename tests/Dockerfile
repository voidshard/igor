FROM golang:1.21

ARG USERNAME=app
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

WORKDIR /go/src/github.com/voidshard/igor
COPY go.mod ./
COPY go.sum ./
COPY cmd cmd
COPY pkg pkg
COPY tests tests
COPY internal internal
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app ./tests/dummy_worker.go && chown -R $USER_UID:$USER_GID /app

USER $USERNAME
CMD ["/app"]
