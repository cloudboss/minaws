use std::{fmt::Display, time::SystemTime};

use aws_sigv4::{
    http_request::{
        sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningError,
        SigningInstructions, SigningSettings,
    },
    sign::v4::SigningParams,
};
use aws_smithy_runtime_api::client::identity::Identity;
use ureq::Request;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SigningError(SigningError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SigningError(e) => write!(f, "Signing error: {}", e),
        }
    }
}

impl From<SigningError> for Error {
    fn from(e: SigningError) -> Self {
        Self::SigningError(e)
    }
}

pub fn sign_request(
    request: Request,
    body: &[u8],
    identity: &Identity,
    region: &str,
    service: &str,
) -> Result<Request> {
    let mut signing_settings = SigningSettings::default();
    if service == "s3" {
        signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    }
    let signing_params = SigningParams::builder()
        .identity(identity)
        .region(region)
        .name(service)
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .unwrap()
        .into();
    let header_names = &request.header_names();
    let headers = header_names.iter().map(|name| {
        let value = request.header(name).unwrap_or("");
        (name.as_ref(), value)
    });
    let signable_request = SignableRequest::new(
        request.method(),
        request.url(),
        headers,
        SignableBody::Bytes(body),
    )?;
    let signing_output = sign(signable_request, &signing_params)?;
    let (signing_instructions, _) = signing_output.into_parts();
    Ok(update_request(request, signing_instructions))
}

fn update_request(mut request: Request, instructions: SigningInstructions) -> Request {
    let (headers, params) = instructions.into_parts();
    for header in headers {
        request = request.set(header.name(), header.value());
    }
    for param in params {
        request = request.query(param.0, &param.1);
    }
    request
}
