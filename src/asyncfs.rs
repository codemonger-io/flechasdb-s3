//! Asynchronous implementation of `flechasdb::io::FileSystem` on Amazon S3.

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumMode;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as base64_engine};
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::StreamReader;

use flechasdb::asyncdb::io::{FileSystem, HashedFileIn};
use flechasdb::error::Error;

/// `FileSystem` on Amazon S3.
pub struct S3FileSystem {
    s3: Client,
    bucket_name: String,
    base_path: String,
}

impl S3FileSystem {
    /// Creates a new `FileSystem` on Amazon S3.
    pub fn new(
        aws_config: &SdkConfig,
        bucket_name: impl Into<String>,
        base_path: impl Into<String>,
    ) -> Self {
        let s3 = Client::new(aws_config);
        S3FileSystem {
            s3,
            bucket_name: bucket_name.into(),
            base_path: base_path.into(),
        }
    }
}

#[async_trait]
impl FileSystem for S3FileSystem {
    type HashedFileIn = S3HashedFileIn;

    async fn open_hashed_file(
        &self,
        path: impl Into<String> + Send,
    ) -> Result<Self::HashedFileIn, Error> {
        Ok(S3HashedFileIn::open(
            self.s3.clone(),
            self.bucket_name.clone(),
            format!("{}/{}", self.base_path, path.into()),
        ))
    }
}

type S3GetObjectResult =
    Result<GetObjectOutput, SdkError<GetObjectError, HttpResponse>>;

pin_project! {
    /// Readable file (object) in an S3 bucket.
    #[must_use = "streams do nothing unless you poll them"]
    pub struct S3HashedFileIn {
        digest: ring::digest::Context,
        #[pin]
        get_object: Pin<Box<dyn Future<Output = S3GetObjectResult> + Send>>,
        checksum: Option<String>,
        body: Option<StreamReader<ByteStream, bytes::Bytes>>,
    }
}

impl S3HashedFileIn {
    fn open(
        s3: Client,
        bucket_name: String,
        key: String,
    ) -> Self {
        let get_object = s3.get_object()
            .bucket(bucket_name)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send();
        S3HashedFileIn {
            digest: ring::digest::Context::new(&ring::digest::SHA256),
            get_object: Box::pin(get_object),
            checksum: None,
            body: None,
        }
    }
}

#[async_trait]
impl HashedFileIn for S3HashedFileIn {
    async fn verify(self) -> Result<(), Error> {
        let digest = self.digest.finish();
        let checksum = base64_engine.encode(digest.as_ref());
        if Some(&checksum) == self.checksum.as_ref() {
            Ok(())
        } else {
            Err(Error::VerificationFailure(format!(
                "checksum discrepancy: expected {:?} but got {}",
                self.checksum,
                checksum,
            )))
        }
    }
}

impl AsyncRead for S3HashedFileIn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.project();
        loop {
            if let Some(body) = this.body.as_mut() {
                // 2. reads the contents
                let last_pos = buf.filled().len();
                return match Pin::new(body).poll_read(cx, buf) {
                    Poll::Ready(Ok(_)) => {
                        if buf.filled().len() > last_pos {
                            let buf = &buf.filled()[last_pos..];
                            this.digest.update(buf);
                        }
                        Poll::Ready(Ok(()))
                    },
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                };
            } else {
                // 1. waits for a response from S3
                match this.get_object.as_mut().poll(cx) {
                    Poll::Ready(Ok(res)) => {
                        if res.checksum_sha256.is_some() {
                            *this.checksum = res.checksum_sha256;
                            *this.body = Some(StreamReader::new(res.body));
                        } else {
                            return Poll::Ready(Err(
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    Error::InvalidContext(format!(
                                        "no checksum for the S3 object",
                                    )),
                                ),
                            ));
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(
                        std::io::Error::new(std::io::ErrorKind::Other, err),
                    )),
                }
            }
        }
    }
}
