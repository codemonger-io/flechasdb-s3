//! Synchronous implementation of `flechasdb::io::FileSystem` on Amazon S3.

use aws_config::SdkConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumMode;
use base64::Engine;
use base64::engine::general_purpose::{
    STANDARD as base64_engine,
    URL_SAFE_NO_PAD as url_safe_base64_engine,
};
use std::io::{Read, Write};
use tempfile::NamedTempFile;

use flechasdb::error::Error;
use flechasdb::io::{FileSystem, HashedFileIn, HashedFileOut};

/// `FileSystem` on Amazon S3.
pub struct S3FileSystem {
    runtime_handle: tokio::runtime::Handle,
    s3: aws_sdk_s3::Client,
    bucket_name: String,
    base_path: String,
}

impl S3FileSystem {
    /// Creates a new `FileSystem` on Amazon S3.
    ///
    /// `runtime_handle` is necessary to wait for requests to Amazon S3.
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        aws_config: &SdkConfig,
        bucket_name: impl Into<String>,
        base_path: impl Into<String>,
    ) -> Self {
        let s3 = Client::new(aws_config);
        Self {
            runtime_handle,
            s3,
            bucket_name: bucket_name.into(),
            base_path: base_path.into(),
        }
    }
}

impl FileSystem for S3FileSystem {
    type HashedFileOut = S3HashedFileOut;
    type HashedFileIn = S3HashedFileIn;

    fn create_hashed_file(&self) -> Result<Self::HashedFileOut, Error> {
        S3HashedFileOut::create(
            self.runtime_handle.clone(),
            self.s3.clone(),
            self.bucket_name.clone(),
            self.base_path.clone(),
        )
    }

    fn create_hashed_file_in(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Self::HashedFileOut, Error> {
        S3HashedFileOut::create(
            self.runtime_handle.clone(),
            self.s3.clone(),
            self.bucket_name.clone(),
            format!("{}/{}", self.base_path, path.as_ref()),
        )
    }

    fn open_hashed_file(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Self::HashedFileIn, Error> {
        S3HashedFileIn::open(
            self.runtime_handle.clone(),
            &self.s3,
            self.bucket_name.clone(),
            format!("{}/{}", self.base_path, path.as_ref()),
        )
    }
}

/// Writable file (object) in an S3 bucket.
///
/// The object key will be the base path plus the URL-safe Base64 encoded
/// SHA-256 hash.
///
/// SHA-256 checksum will also be set.
pub struct S3HashedFileOut {
    runtime_handle: tokio::runtime::Handle,
    s3: Client,
    tempfile: NamedTempFile,
    bucket_name: String,
    base_path: String,
    digest: ring::digest::Context,
}

impl S3HashedFileOut {
    fn create(
        runtime_handle: tokio::runtime::Handle,
        s3: Client,
        bucket_name: String,
        base_path: String,
    ) -> Result<Self, Error> {
        let tempfile = NamedTempFile::new()?;
        Ok(S3HashedFileOut {
            runtime_handle,
            s3,
            tempfile,
            bucket_name,
            base_path,
            digest: ring::digest::Context::new(&ring::digest::SHA256),
        })
    }
}

impl Write for S3HashedFileOut {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.digest.update(buf);
        self.tempfile.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.tempfile.flush()
    }
}

impl HashedFileOut for S3HashedFileOut {
    /// Uploads the contents to the S3 bucket.
    ///
    /// Blocks until the upload completes.
    /// This function must be called within the context of a Tokio runtime,
    /// otherwise fails with `Error::InvalidContext`.
    fn persist(mut self, extension: impl AsRef<str>) -> Result<String, Error> {
        self.flush()?;
        let digest = self.digest.finish();
        let id = url_safe_base64_engine.encode(digest.as_ref());
        let checksum = base64_engine.encode(digest.as_ref());
        let key = format!("{}/{}.{}", self.base_path, id, extension.as_ref());
        let body = self.runtime_handle
            .block_on(ByteStream::from_path(self.tempfile.path()))
            .map_err(|e| Error::InvalidContext(
                format!("failed to read the temporary file: {}", e),
            ))?;
        let res = self.s3.put_object()
            .bucket(self.bucket_name)
            .key(key)
            .checksum_sha256(checksum)
            .body(body)
            .send();
        self.runtime_handle
            .block_on(res)
            .map_err(|e| Error::InvalidContext(
                format!("failed to upload the content to S3: {}", e),
            ))?;
        Ok(id)
    }
}

/// Readable file (object) in an S3 bucket.
pub struct S3HashedFileIn {
    body: bytes::Bytes,
    read_pos: usize,
    checksum: String,
    digest: ring::digest::Context,
}

impl S3HashedFileIn {
    /// Downloads the contents from an S3 bucket.
    ///
    /// Blocks until the download completes.
    /// This function must be called within the context of a Tokio runtime,
    /// otherwise fails with `Error::InvalidContext`.
    fn open(
        runtime_handle: tokio::runtime::Handle,
        s3: &Client,
        bucket_name: String,
        key: String,
    ) -> Result<Self, Error> {
        let res = s3.get_object()
            .bucket(bucket_name)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send();
        let res = runtime_handle
            .block_on(res)
            .map_err(|e| Error::InvalidContext(
                format!("failed to request the content from S3: {}", e),
            ))?;
        let checksum = res.checksum_sha256
            .ok_or(Error::InvalidContext(
                format!("no checksum for the content from S3"),
            ))?;
        let body = runtime_handle
            .block_on(res.body.collect())
            .map_err(|e| Error::InvalidContext(
                format!("failed to read the content from S3: {}", e),
            ))?
            .into_bytes();
        Ok(S3HashedFileIn {
            body,
            read_pos: 0,
            checksum,
            digest: ring::digest::Context::new(&ring::digest::SHA256),
        })
    }
}

impl Read for S3HashedFileIn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut stream = &self.body[self.read_pos..];
        let n = stream.read(buf)?;
        self.read_pos += n;
        self.digest.update(&buf[..n]);
        Ok(n)
    }
}

impl HashedFileIn for S3HashedFileIn {
    fn verify(self) -> Result<(), Error> {
        let digest = self.digest.finish();
        let checksum = base64_engine.encode(digest.as_ref());
        if checksum == self.checksum {
            Ok(())
        } else {
            Err(Error::VerificationFailure(format!(
                "checsum discrepancy: expected {} but got {}",
                self.checksum,
                checksum,
            )))
        }
    }
}
