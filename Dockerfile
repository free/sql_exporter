FROM alpine
EXPOSE 9399
RUN mkdir /app
WORKDIR /app
COPY sql_exporter /app
ENTRYPOINT ["./sql_exporter"]
