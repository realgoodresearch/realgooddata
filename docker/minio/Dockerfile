FROM alpine:3.22

RUN apk add --no-cache ca-certificates curl

COPY minio /usr/bin/minio

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data"]
