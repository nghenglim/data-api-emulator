## README
- currently hard convert tiny to boolean, which should be what aws is doing
- currently not sure is convertion of DATE and TIME value is correct
- not 100% emulate, just make most of the stuff works

## Run with docker-compose
`docker-compose up`

~~~
version: '2'

services:
  data-api-emulator:
    image: nghenglim/data-api-emulator:0.1.1
    environment:
      MYSQL_HOST: db
      MYSQL_PORT: 3306
      MYSQL_USER: root
      MYSQL_PASSWORD: example
      RESOURCE_ARN: 'arn:aws:rds:us-east-1:123456789012:cluster:dummy'
      SECRET_ARN: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'
    ports:
      - "8080:8080"
  db:
    image: mysql:5.6
    command: --default-authentication-plugin=mysql_native_password
    environment:
      MYSQL_ROOT_PASSWORD: example
      MYSQL_DATABASE: test
    ports:
        - "3306:3306"
~~~

## building
~~~
docker run --net=host --rm -it -v "$(pwd)/cache/git:/home/rust/.cargo/git" -v "$(pwd)/cache/registry:/home/rust/.cargo/registry" -v "$(pwd)/Cargo.lock:/home/rust/src/Cargo.lock" -v "$(pwd)/Cargo.toml:/home/rust/src/Cargo.toml" -v "$(pwd)/target:/home/rust/src/target" -v "$(pwd)/src:/home/rust/src/src" ekidd/rust-musl-builder  bash -c "sudo chown -R rust:rust /home/rust/.cargo/git /home/rust/.cargo/registry /home/rust/src/target && cargo build --release"
docker build -t nghenglim/data-api-emulator:0.1.2 .
docker push nghenglim/data-api-emulator:0.1.2
~~~

## Running
usage for mac: `docker run -p 8080:8080 -e MYSQL_HOST=docker.for.mac.localhost  -it nghenglim/data-api-emulator:0.0.2`

from cargo
~~~
cp .env.example .env
cargo run
~~~

## Docker Build
~~~
docker build -t local/data-api-emulator . && docker run --rm -it local/data-api-emulator
~~~

## a little testing
docker-compose up
cargo run
cargo test -- --test-threads 1
