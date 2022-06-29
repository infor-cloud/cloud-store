# syntax=docker/dockerfile:1
FROM alpine:3.14 AS cloud-store-deps
RUN apk update && apk add bash wget unzip
WORKDIR /download
COPY download-deps.sh .
RUN bash download-deps.sh

FROM lb-database-dist as cloud-store
WORKDIR /app
COPY --from=cloud-store-deps /usr/local/lib/java/* /usr/local/lib/java/
COPY . .
ENV LB_UNIVERSE_DEPS=/usr/local/
ENV CLOUDSTORE_HOME=/cloud-store
RUN ./configure --prefix=/cloud-store && make && make install && cd / && rm -r /app