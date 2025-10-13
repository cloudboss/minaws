use std::{
    error,
    fmt::{Debug, Display},
    io,
    thread::sleep,
    time::Duration,
};

use aws_credential_types::Credentials;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use ureq::Response;

use crate::request::{self, sign_request};

const SERVICE_NAME: &str = "ec2";

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Api(u16, Box<Response>),
    Http(Box<ureq::Error>),
    Io(io::Error),
    Json(serde_json::Error),
    Request(request::Error),
    EC2(ErrorBody),
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

#[derive(Clone, Debug)]
pub struct Api {
    credentials: Credentials,
    region: String,
}

impl Api {
    pub fn new(region: &str, credentials: Credentials) -> Self {
        Self {
            region: region.into(),
            credentials,
        }
    }

    pub fn attach_volume(&self, input: AttachVolumeInput) -> Result<AttachVolumeOutput> {
        let req = ureq::post(&self.url());

        let params = vec![
            ("Action".into(), "AttachVolume".into()),
            ("Version".into(), "2016-11-15".into()),
            ("Device".into(), input.device),
            ("InstanceId".into(), input.instance_id),
            ("VolumeId".into(), input.volume_id),
        ];

        match self.send(req, params) {
            Ok(response) => {
                let body = response.into_reader();
                let output = serde_xml_rs::from_reader(body)?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let body = response.into_reader();
                let err_body = serde_xml_rs::from_reader(body)?;
                Err(Error::EC2(err_body))
            }
            Err(err) => Err(err),
        }
    }

    pub fn describe_volumes(&self, input: DescribeVolumesInput) -> Result<DescribeVolumesOutput> {
        let url = &self.url();
        let req = ureq::post(&format!("{}/", url));

        let mut params = vec![
            ("Action".into(), "DescribeVolumes".into()),
            ("Version".into(), "2016-11-15".into()),
        ];
        if let Some(filters) = input.filters {
            params.extend(filters.to_params("Filter"));
        }
        if let Some(max_results) = input.max_results {
            params.push(("MaxResults".into(), max_results.to_string()));
        }
        if let Some(next_token) = input.next_token {
            params.push(("NextToken".into(), next_token));
        }
        if let Some(volume_ids) = input.volume_ids {
            params.extend(volume_ids.to_params("VolumeId"));
        }

        match self.send(req, params) {
            Ok(response) => {
                let body = response.into_reader();
                let output = serde_xml_rs::from_reader(body)?;
                Ok(output)
            }
            Err(Error::Api(_, response)) => {
                let body = response.into_reader();
                let err_body = serde_xml_rs::from_reader(body)?;
                Err(Error::EC2(err_body))
            }
            Err(err) => Err(err),
        }
    }

    fn send(
        &self,
        mut req: ureq::Request,
        params: Vec<(String, String)>,
    ) -> Result<ureq::Response> {
        let params_ref: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let encoded_params = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params_ref.iter())
            .finish();
        let body = encoded_params.as_bytes();

        let identity = self.credentials.clone().into();
        req = sign_request(req, body, &identity, &self.region, SERVICE_NAME)?;

        let mut retries = 0;
        loop {
            match req.clone().send_bytes(body).map_err(Into::into) {
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

trait ToParams {
    fn to_params(&self, prefix: &str) -> Vec<(String, String)>;
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Filter {
    pub name: String,
    pub values: Vec<String>,
}

impl ToParams for Filter {
    fn to_params(&self, prefix: &str) -> Vec<(String, String)> {
        let mut params = Vec::new();
        params.push((format!("{}.Name", prefix), self.name.clone()));
        for (i, val) in self.values.iter().enumerate() {
            params.push((format!("{}.Value.{}", prefix, i + 1), val.clone()));
        }
        params
    }
}

impl ToParams for Vec<Filter> {
    fn to_params(&self, prefix: &str) -> Vec<(String, String)> {
        let mut params = Vec::new();
        for (i, filter) in self.iter().enumerate() {
            let filter_prefix = format!("{}.{}", prefix, i + 1);
            params.extend(filter.to_params(&filter_prefix));
        }
        params
    }
}

impl ToParams for Vec<String> {
    fn to_params(&self, prefix: &str) -> Vec<(String, String)> {
        let mut params = Vec::new();
        for (i, value) in self.iter().enumerate() {
            let prefix = format!("{}.{}", prefix, i + 1);
            params.push((prefix, value.clone()));
        }
        params
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttachVolumeInput {
    #[serde(rename = "Device")]
    pub device: String,
    #[serde(rename = "InstanceId")]
    pub instance_id: String,
    #[serde(rename = "VolumeId")]
    pub volume_id: String,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttachVolumeOutput {
    #[serde(rename = "associatedResource")]
    pub associated_resource: Option<String>,
    #[serde(rename = "attachTime")]
    pub attach_time: Option<DateTime<Utc>>,
    #[serde(rename = "deleteOnTermination")]
    pub delete_on_termination: Option<bool>,
    #[serde(rename = "device")]
    pub device: Option<String>,
    #[serde(rename = "instanceId")]
    pub instance_id: Option<String>,
    #[serde(rename = "instanceOwningService")]
    pub instance_owning_service: Option<Status>,
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
    #[serde(rename = "status")]
    pub status: Option<Status>,
    #[serde(rename = "volumeId")]
    pub volume_id: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DescribeVolumesInput {
    pub filters: Option<Vec<Filter>>,
    pub max_results: Option<u32>,
    pub next_token: Option<String>,
    pub volume_ids: Option<Vec<String>>,
}

impl DescribeVolumesInput {
    pub fn filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters = Some(filters);
        self
    }

    pub fn max_results(mut self, max_results: u32) -> Self {
        self.max_results = Some(max_results);
        self
    }

    pub fn next_token(mut self, next_token: &str) -> Self {
        self.next_token = Some(next_token.into());
        self
    }

    pub fn volume_ids(mut self, volume_ids: Vec<String>) -> Self {
        self.volume_ids = Some(volume_ids);
        self
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DescribeVolumesOutput {
    #[serde(rename = "nextToken")]
    pub next_token: Option<String>,
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
    #[serde(rename = "volumeSet")]
    pub volumes: Option<VolumeSet>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct VolumeSet {
    #[serde(rename = "item")]
    pub items: Option<Vec<Volume>>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Volume {
    #[serde(rename = "attachments")]
    pub attachments: Option<AttachmentSet>,
    #[serde(rename = "availabilityZone")]
    pub availability_zone: Option<String>,
    #[serde(rename = "availabilityZoneId")]
    pub availability_zone_id: Option<String>,
    #[serde(rename = "createTime")]
    pub create_time: Option<String>,
    #[serde(rename = "encrypted")]
    pub encrypted: Option<bool>,
    #[serde(rename = "iops")]
    pub iops: Option<u32>,
    #[serde(rename = "kmsKeyId")]
    pub kms_key_id: Option<String>,
    #[serde(rename = "size")]
    pub size: Option<u32>,
    #[serde(rename = "snapshotId")]
    pub snapshot_id: Option<String>,
    #[serde(rename = "state")]
    pub state: Option<String>,
    #[serde(rename = "volumeId")]
    pub volume_id: Option<String>,
    #[serde(rename = "volumeType")]
    pub volume_type: Option<String>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttachmentSet {
    #[serde(rename = "item")]
    pub items: Option<Vec<Attachment>>,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Attachment {
    #[serde(rename = "associatedResource")]
    pub associated_resource: Option<String>,
    #[serde(rename = "attachTime")]
    pub attach_time: Option<DateTime<Utc>>,
    #[serde(rename = "deleteOnTermination")]
    pub delete_on_termination: Option<bool>,
    #[serde(rename = "device")]
    pub device: Option<String>,
    #[serde(rename = "instanceId")]
    pub instance_id: Option<String>,
    #[serde(rename = "instanceOwningService")]
    pub instance_owning_service: Option<Status>,
    #[serde(rename = "status")]
    pub status: Option<String>,
    #[serde(rename = "volumeId")]
    pub volume_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Attaching,
    Attached,
    Detaching,
    Detached,
    Busy,

    #[serde(other)]
    #[default]
    Unknown,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ErrorBody {
    #[serde(rename = "Errors")]
    pub errors: Errors,
    #[serde(rename = "RequestID")]
    pub request_id: String,
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Errors {
    #[serde(rename = "Error")]
    pub error: Vec<ApiError>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ApiError {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
}
