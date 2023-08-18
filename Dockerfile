FROM golang:1.20.5-alpine AS build-env
RUN apk add --no-cache bash
RUN apk add make git bash build-base
ENV GOPATH=/go
ENV PATH="/go/bin:${PATH}"
ADD ./ /go/src/github.com/bmeg/grip
RUN cd /go/src/github.com/bmeg/grip && make install

# final stage
FROM alpine
WORKDIR /data
VOLUME /data
ENV PATH="/app:${PATH}"
COPY --from=build-env /go/bin/grip /app/