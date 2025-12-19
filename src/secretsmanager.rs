use std::{error, fmt::Display};

use aws_credential_types::Credentials;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::request::{self, sign_request, with_retry};

const SERVICE_NAME: &str = "secretsmanager";

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Api(u16, Box<Response>),
    Json(serde_json::Error),
    Request(request::Error),
    SecretsManager(ErrorBody),
    Transport(Box<ureq::Error>),
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err)
    }
}

impl From<request::Error> for Error {
    fn from(err: request::Error) -> Self {
        match err {
            request::Error::Api(_, response) => {
                let body = response.into_reader();
                match serde_json::from_reader(body) {
                    Ok(err_body) => Error::SecretsManager(err_body),
                    Err(e) => Error::Json(e),
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

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ErrorBody {
    #[serde(rename = "__type")]
    pub r#type: String,
    #[serde(rename = "Message")]
    pub message: Option<String>,
}

impl Display for ErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
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

    pub fn get_secret_value(&self, input: GetSecretValueInput) -> Result<GetSecretValueOutput> {
        let mut req = ureq::post(&self.url());
        req = req.set("Content-Type", "application/x-amz-json-1.1");
        req = req.set("X-Amz-Target", "secretsmanager.GetSecretValue");
        self.send(req, input)
            .and_then(|response| {
                let output = serde_json::from_reader(response.into_reader())?;
                Ok(output)
            })
            .map_err(Into::into)
    }

    fn send<I: Serialize>(&self, mut req: ureq::Request, input: I) -> Result<Response> {
        let body = serde_json::to_vec(&input)?;
        let identity = self.credentials.clone().into();
        req = sign_request(req, &body, &identity, &self.region, SERVICE_NAME)?;
        with_retry(|| req.clone().send_bytes(&body), 5).map_err(Into::into)
    }

    fn url(&self) -> String {
        format!("https://{}.{}.amazonaws.com", SERVICE_NAME, self.region)
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GetSecretValueInput {
    #[serde(rename = "SecretId")]
    pub secret_id: String,
    #[serde(rename = "VersionId")]
    pub version_id: Option<String>,
    #[serde(rename = "VersionStage")]
    pub version_stage: Option<String>,
}

impl GetSecretValueInput {
    pub fn secret_id(mut self, secret_id: &str) -> Self {
        self.secret_id = secret_id.into();
        self
    }
    pub fn version_id(mut self, version_id: &str) -> Self {
        self.version_id = Some(version_id.into());
        self
    }
    pub fn version_stage(mut self, version_stage: &str) -> Self {
        self.version_stage = Some(version_stage.into());
        self
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GetSecretValueOutput {
    #[serde(rename = "ARN")]
    pub arn: String,
    #[serde(rename = "CreatedDate")]
    pub created_date: f64,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "SecretBinary")]
    pub secret_binary: Option<Vec<u8>>,
    #[serde(rename = "SecretString")]
    pub secret_string: Option<String>,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "VersionStages")]
    pub version_stages: Vec<String>,
}
