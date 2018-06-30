FROM        alpine:3.7
WORKDIR     /exporter/
RUN         apk --update add --no-cache libaio

ADD         https://github.com/Corundex/database_exporter/releases/download/0.6.6/database_exporter.tar.gz /exporter/
RUN         tar -xzvf /exporter/database_exporter.tar.gz

EXPOSE      9285

ENTRYPOINT  [ "/exporter/database_exporter" ]

