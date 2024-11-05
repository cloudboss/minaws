use std::{error, fmt::Display, thread::sleep, time::Duration};

use aws_credential_types::Credentials;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::request::{self, sign_request};

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

impl From<ureq::Error> for Error {
    fn from(err: ureq::Error) -> Self {
        match err {
            ureq::Error::Status(status, response) => Error::Api(status, Box::new(response)),
            ureq::Error::Transport(_) => Error::Transport(Box::new(err)),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Json(err)
    }
}

impl From<request::Error> for Error {
    fn from(err: request::Error) -> Self {
        Error::Request(err)
    }
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
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

#[derive(Debug, Clone)]
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
        match self.send(req, input) {
            Ok(response) => {
                let output = serde_json::from_reader(response.into_reader())?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let err_body = serde_json::from_reader(response.into_reader())?;
                Err(Error::SecretsManager(err_body))
            }
            Err(err) => Err(err),
        }
    }

    fn send<I: Serialize>(&self, mut req: ureq::Request, input: I) -> Result<ureq::Response> {
        let body = serde_json::to_vec(&input)?;
        let identity = self.credentials.clone().into();
        req = sign_request(req, &body, &identity, &self.region, SERVICE_NAME)?;

        let mut retries = 0;
        loop {
            match req.clone().send_bytes(&body).map_err(Into::into) {
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

    fn url(&self) -> String {
        format!("https://{}.{}.amazonaws.com", SERVICE_NAME, self.region)
    }
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
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
#[derive(Debug, Default, Deserialize, Serialize)]
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
