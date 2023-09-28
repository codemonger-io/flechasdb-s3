# flechasdb-s3

An Amazon S3 extension for the FlechasDB system.

This library is intended to be combined with [`flechasdb`](https://github.com/codemonger-io/flechasdb).

## Installing flechasdb-s3

There is no crate published yet.
Please add the following line to your `Cargo.toml` file:
```toml
flechasdb-s3 = { git = "https://github.com/codemonger-io/flechasdb-s3" }
```

## API documentation

https://codemonger-io.github.io/flechasdb-s3/api/flechasdb_s3/

## Development

### Building the library

```sh
cargo build
```

### Generating the API documentation

```sh
cargo doc --lib --no-deps --release
```