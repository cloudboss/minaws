use std::{error, fmt::Display, thread::sleep, time::Duration};

use aws_credential_types::Credentials;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::request::{self, sign_request};

const SERVICE_NAME: &str = "ssm";

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Api(u16, Box<Response>),
    Json(serde_json::Error),
    Request(request::Error),
    SSM(ErrorBody),
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

    pub fn get_parameter(&self, input: GetParameterInput) -> Result<GetParameterOutput> {
        let mut req = ureq::post(&self.url());
        req = req.set("Content-Type", "application/x-amz-json-1.1");
        req = req.set("X-Amz-Target", "AmazonSSM.GetParameter");
        match self.send(req, input) {
            Ok(response) => {
                let output = serde_json::from_reader(response.into_reader())?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let err_body = serde_json::from_reader(response.into_reader())?;
                Err(Error::SSM(err_body))
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_parameters_by_path(
        &self,
        input: GetParametersByPathInput,
    ) -> Result<GetParametersByPathOutput> {
        let mut req = ureq::post(&self.url());
        req = req.set("Content-Type", "application/x-amz-json-1.1");
        req = req.set("X-Amz-Target", "AmazonSSM.GetParametersByPath");
        match self.send(req, input) {
            Ok(response) => {
                let output = serde_json::from_reader(response.into_reader())?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let err_body = serde_json::from_reader(response.into_reader())?;
                Err(Error::SSM(err_body))
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
pub struct GetParameterInput {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "WithDecryption")]
    pub with_decryption: Option<bool>,
}

impl GetParameterInput {
    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }
    pub fn with_decryption(mut self, with_decryption: bool) -> Self {
        self.with_decryption = Some(with_decryption);
        self
    }
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct GetParameterOutput {
    #[serde(rename = "Parameter")]
    pub parameter: Option<Parameter>,
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct GetParametersByPathInput {
    #[serde(rename = "MaxResults")]
    pub max_results: Option<u32>,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
    #[serde(rename = "ParameterFilters")]
    pub parameter_filters: Option<Vec<ParameterStringFilter>>,
    #[serde(rename = "Path")]
    pub path: String,
    #[serde(rename = "Recursive")]
    pub recursive: Option<bool>,
    #[serde(rename = "WithDecryption")]
    pub with_decryption: Option<bool>,
}

impl GetParametersByPathInput {
    pub fn max_results(mut self, max_results: u32) -> Self {
        self.max_results = Some(max_results);
        self
    }
    pub fn next_token(mut self, next_token: &str) -> Self {
        self.next_token = Some(next_token.to_string());
        self
    }
    pub fn parameter_filters(mut self, parameter_filters: Vec<ParameterStringFilter>) -> Self {
        self.parameter_filters = Some(parameter_filters);
        self
    }
    pub fn path(mut self, path: &str) -> Self {
        self.path = path.to_string();
        self
    }
    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = Some(recursive);
        self
    }
    pub fn with_decryption(mut self, with_decryption: bool) -> Self {
        self.with_decryption = Some(with_decryption);
        self
    }
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ParameterStringFilter {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Option")]
    pub option: Option<String>,
    #[serde(rename = "Values")]
    pub values: Option<Vec<String>>,
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct GetParametersByPathOutput {
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
    #[serde(rename = "Parameters")]
    pub parameters: Option<Vec<Parameter>>,
}

#[skip_serializing_none]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Parameter {
    #[serde(rename = "ARN")]
    pub arn: Option<String>,
    #[serde(rename = "DataType")]
    pub data_type: Option<String>,
    #[serde(rename = "LastModifiedDate")]
    pub last_modified_date: Option<f64>,
    #[serde(rename = "Name")]
    pub name: Option<String>,
    #[serde(rename = "Selector")]
    pub selector: Option<String>,
    #[serde(rename = "SourceResult")]
    pub source_result: Option<String>,
    #[serde(rename = "Type")]
    pub r#type: Option<String>,
    #[serde(rename = "Value")]
    pub value: Option<String>,
}
