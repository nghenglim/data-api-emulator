FROM scratch
COPY target/x86_64-unknown-linux-musl/release/data-api-local .
USER 1000
ENV RESOURCE_ARN="arn:aws:rds:us-east-1:123456789012:cluster:dummy"
ENV SECRET_ARN="arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy"
ENV MYSQL_HOST="localhost"
ENV MYSQL_PORT="3306"
ENV MYSQL_USER="root"
ENV MYSQL_PASSWORD="example"
ENV HOST="0.0.0.0"
ENV PORT="8080"
ENV JSONLIMIT="99999999"
ENV RUST_LOG="actix_web=debug"
CMD ["./data-api-local"]
