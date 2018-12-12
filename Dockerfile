# build stage
FROM golang:alpine AS build-env
# RUN apt-get update && apt-get install -y golang make git
RUN apk add make git bash
ENV GOPATH=/go
ENV PATH="/go/bin:${PATH}"
ADD . /go/src/github.com/bmeg/grip
RUN cd /go/src/github.com/bmeg/grip && make depends && make

# final stage
FROM alpine
WORKDIR /data
VOLUME /data
ENV PATH="/app:${PATH}"
COPY --from=build-env /go/bin/grip /app/
