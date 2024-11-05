use std::{
    error,
    fmt::{Debug, Display, Formatter},
    io::{self, Read},
    thread::sleep,
    time::Duration,
};

use aws_credential_types::Credentials;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::{
    imds,
    request::{self, sign_request},
};

const SERVICE_NAME: &str = "s3";

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Api(u16, Box<Response>),
    Http(Box<ureq::Error>),
    Imds(Box<imds::Error>),
    Io(io::Error),
    NoSuchBucket,
    Request(request::Error),
    S3(ErrorBody),
    Transport(Box<ureq::Error>),
    Xml(serde_xml_rs::Error),
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<ureq::Error> for Error {
    fn from(err: ureq::Error) -> Self {
        match err {
            ureq::Error::Status(status, response) => Error::Api(status, Box::new(response)),
            ureq::Error::Transport(_) => Error::Transport(Box::new(err)),
        }
    }
}

impl From<imds::Error> for Error {
    fn from(err: imds::Error) -> Self {
        Error::Imds(Box::new(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<request::Error> for Error {
    fn from(err: request::Error) -> Self {
        Error::Request(err)
    }
}

impl From<serde_xml_rs::Error> for Error {
    fn from(err: serde_xml_rs::Error) -> Self {
        Error::Xml(err)
    }
}

#[derive(Debug)]
pub struct Api {
    credentials: Credentials,
    region: String,
}

impl Api {
    pub fn new(region: &str, credentials: Credentials) -> Self {
        Self {
            region: region.to_string(),
            credentials,
        }
    }

    pub fn list_objects_v2(&self, input: ListObjectsV2Input) -> Result<ListObjectsV2Output> {
        let url = &self.url(&input.bucket);
        let mut req = ureq::get(&format!("{}/", url));
        req = req.query("list-type", "2");
        if let Some(continuation_token) = input.continuation_token {
            req = req.query("continuation-token", &continuation_token);
        }
        if let Some(prefix) = input.prefix {
            req = req.query("prefix", &prefix);
        }
        match self.send(req) {
            Ok(response) => {
                let body = response.into_reader();
                let output = serde_xml_rs::from_reader(body)?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let body = response.into_reader();
                let err_body = serde_xml_rs::from_reader(body)?;
                Err(Error::S3(err_body))
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_object(&self, input: GetObjectInput) -> Result<GetObjectOutput> {
        let url = &self.url(&input.bucket);
        let req = ureq::get(&format!("{}/{}", url, input.key));
        match self.send(req) {
            Ok(response) => Ok(GetObjectOutput {
                body: response.into_reader(),
            }),
            Err(Error::Api(_, response)) => {
                let body = response.into_reader();
                let err_body = serde_xml_rs::from_reader(body)?;
                Err(Error::S3(err_body))
            }
            Err(err) => Err(err),
        }
    }

    fn send(&self, mut req: ureq::Request) -> Result<ureq::Response> {
        let identity = self.credentials.clone().into();
        req = sign_request(req, &[], &identity, &self.region, SERVICE_NAME)?;

        let mut retries = 0;
        loop {
            match req.clone().call().map_err(Into::into) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if retries >= 3 {
                        return Err(e);
                    }
                    if retries > 0 {
                        sleep(Duration::from_millis(retries * 10));
                    }
                    retries += 1;
                }
            }
        }
    }

    fn url(&self, bucket: &str) -> String {
        format!(
            "https://{}.{}.{}.amazonaws.com",
            bucket, SERVICE_NAME, self.region
        )
    }
}

#[derive(Debug, Default)]
pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
}

impl GetObjectInput {
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.bucket = bucket.to_string();
        self
    }

    pub fn key(mut self, key: &str) -> Self {
        self.key = key.to_string();
        self
    }
}

pub struct GetObjectOutput {
    pub body: Box<dyn Read + Send + Sync + 'static>,
}

impl Debug for GetObjectOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetObjectOutput")
            .field("body", &"[..]")
            .finish()
    }
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ListObjectsV2Input {
    pub bucket: String,
    pub continuation_token: Option<String>,
    pub prefix: Option<String>,
}

impl ListObjectsV2Input {
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.bucket = bucket.to_string();
        self
    }

    pub fn continuation_token(mut self, continuation_token: &str) -> Self {
        self.continuation_token = Some(continuation_token.to_string());
        self
    }

    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListObjectsV2Output {
    #[serde(rename = "CommonPrefixes")]
    pub common_prefixes: Option<Vec<CommonPrefix>>,
    #[serde(rename = "Contents")]
    pub contents: Option<Vec<Object>>,
    #[serde(rename = "ContinuationToken")]
    pub continuation_token: Option<String>,
    #[serde(rename = "Delimiter")]
    pub delimiter: Option<String>,
    #[serde(rename = "EncodingType")]
    pub encoding_type: Option<String>,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: Option<bool>,
    #[serde(rename = "KeyCount")]
    pub key_count: Option<u32>,
    #[serde(rename = "MaxKeys")]
    pub max_keys: Option<u32>,
    #[serde(rename = "Name")]
    pub name: Option<String>,
    #[serde(rename = "NextContinuationToken")]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
    #[serde(rename = "StartAfter")]
    pub start_after: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    prefix: String,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct Object {
    #[serde(rename = "ChecksumAlgorithm")]
    pub checksum_algorithm: Option<Vec<String>>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<Vec<String>>,
    #[serde(rename = "Key")]
    pub key: Option<String>,
    #[serde(rename = "LastModified")]
    pub last_modified: Option<DateTime<Utc>>,
    #[serde(rename = "Owner")]
    pub owner: Option<String>,
    #[serde(rename = "RestoreStatus")]
    pub restore_status: Option<String>,
    #[serde(rename = "Size")]
    pub size: Option<i64>,
    #[serde(rename = "StorageClass")]
    pub storage_class: Option<String>,
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ErrorBody {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: Option<String>,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}
