FROM        quay.io/prometheus/busybox:glibc
MAINTAINER  Anatoly Butko
WORKDIR /bin 
ADD https://github.com/Corundex/database_exporter/releases/download/0.6.3/database_exporter.tar.gz /bin
RUN tar -xzvf database_exporter.tar.gz
COPY /bin/config/mysql_exporter.yml /bin/database_exporter.yml
EXPOSE      9285
ENTRYPOINT  [ "/bin/database_exporter" ]
