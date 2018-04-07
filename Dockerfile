FROM        quay.io/prometheus/busybox:glibc
MAINTAINER  Anatoly Butko
WORKDIR /bin 
ADD https://github.com/Corundex/database_exporter/releases/download/0.6.3/database_exporter.tar.gz /tmp/
ADD /tmp/database_exporter.tar.gz /bin/
COPY /bin/config/mysql_exporter.yml /bin/database_exporter.yml
COPY /bin/mysql_collectors/*.yml /bin/
EXPOSE      9285
ENTRYPOINT  [ "/bin/database_exporter" ]
