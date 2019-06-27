// Originally from https://github.com/rust-lang/crates.io/blob/master/src/s3/lib.rs
//#![deny(warnings)]

#[allow(unused_imports, deprecated)]
use std::ascii::AsciiExt;
use std::fmt;

use base64;
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha1::Sha1;
use futures::{Future, Stream};
use hyper::header::HeaderValue;
use hyper::Method;
use hyperx::header;
use reqwest;
use rusoto_s3::S3;
use simples3::credential::*;
use time;

use errors::*;
use util::HeadersExt;

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
/// Whether or not to use SSL.
pub enum Ssl {
    /// Use SSL.
    Yes,
    /// Do not use SSL.
    No,
}

fn base_url(endpoint: &str, ssl: Ssl) -> String {
    format!(
        "{}://{}/",
        match ssl {
            Ssl::Yes => "https",
            Ssl::No => "http",
        },
        endpoint
    )
}

fn hmac<D: Digest>(d: D, key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut hmac = Hmac::new(d, key);
    hmac.input(data);
    hmac.result().code().iter().map(|b| *b).collect::<Vec<u8>>()
}

fn signature(string_to_sign: &str, signing_key: &str) -> String {
    let s = hmac(
        Sha1::new(),
        signing_key.as_bytes(),
        string_to_sign.as_bytes(),
    );
    base64::encode_config::<Vec<u8>>(&s, base64::STANDARD)
}

/// An S3 bucket.
pub struct Bucket {
    name: String,
    base_url: String,
    client: rusoto_s3::S3Client,
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bucket(name={}, base_url={})", self.name, self.base_url)
    }
}

impl Bucket {
    pub fn new(name: &str, endpoint: &str, ssl: Ssl) -> Result<Bucket> {
        let base_url = base_url(&endpoint, ssl);
        Ok(Bucket {
            name: name.to_owned(),
            base_url: base_url,
            client: rusoto_s3::S3Client::new(rusoto_core::Region::UsEast1),
        })
    }

    pub fn get(&self, key: &str, creds: Option<&AwsCredentials>) -> SFuture<Vec<u8>> {
        Box::new(
            self.client
                .get_object(rusoto_s3::GetObjectRequest {
                    bucket: self.name.clone(),
                    key: "".into(),
                    ..Default::default()
                })
                .map_err(|err| err.to_string().into())
                .and_then(|response| {
                    response
                        .body
                        .unwrap()
                        .concat()
                        .map(|b| b.to_vec())
                        .map_err(|err| err.to_string().into())
                }),
        )
    }

    pub fn put(&self, key: &str, content: Vec<u8>, creds: &AwsCredentials) -> SFuture<()> {
        Box::new(
            self.client
                .put_object(rusoto_s3::PutObjectRequest {
                    bucket: self.name.clone(),
                    key: key.into(),
                    body: Some(content.into()),
                    ..Default::default()
                })
                .map_err(|err| err.to_string().into())
                .map(|_| ()),
        )
    }
}
