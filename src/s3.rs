use std::{
    error,
    fmt::{Debug, Display, Formatter},
    io::{self, Read},
};

use aws_credential_types::Credentials;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::request::{self, sign_request, with_retry};

const SERVICE_NAME: &str = "s3";

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Request(request::Error),
    S3(ErrorBody),
    Xml(serde_xml_rs::Error),
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {}", e),
            Self::Request(e) => write!(f, "http request error: {}", e),
            Self::S3(eb) => write!(f, "s3 error: {}", eb.message),
            Self::Xml(e) => write!(f, "xml error: {}", e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<request::Error> for Error {
    fn from(err: request::Error) -> Self {
        match err {
            request::Error::Api(_, response) => {
                let body = response.into_reader();
                match serde_xml_rs::from_reader(body) {
                    Ok(err_body) => Error::S3(err_body),
                    Err(e) => Error::Xml(e),
                }
            }
            request::Error::SigningError(signing_error) => {
                Error::Request(request::Error::SigningError(signing_error))
            }
            request::Error::Transport(transport_error) => {
                Error::Request(request::Error::Transport(transport_error))
            }
        }
    }
}

impl From<serde_xml_rs::Error> for Error {
    fn from(err: serde_xml_rs::Error) -> Self {
        Error::Xml(err)
    }
}

#[derive(Clone, Debug)]
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
        self.send(req)
            .and_then(|response| {
                let body = response.into_reader();
                let output = serde_xml_rs::from_reader(body)?;
                Ok(output)
            })
            .map_err(Into::into)
    }

    pub fn get_object(&self, input: GetObjectInput) -> Result<GetObjectOutput> {
        let url = &self.url(&input.bucket);
        let req = ureq::get(&format!("{}/{}", url, input.key));
        self.send(req)
            .and_then(|response| {
                Ok(GetObjectOutput {
                    body: response.into_reader(),
                })
            })
            .map_err(Into::into)
    }

    fn send(&self, mut req: ureq::Request) -> Result<Response> {
        let identity = self.credentials.clone().into();
        req = sign_request(req, &[], &identity, &self.region, SERVICE_NAME)?;
        with_retry(|| req.clone().call(), 5).map_err(Into::into)
    }

    fn url(&self, bucket: &str) -> String {
        format!(
            "https://{}.{}.{}.amazonaws.com",
            bucket, SERVICE_NAME, self.region
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    prefix: String,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
