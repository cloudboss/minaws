use std::collections::HashMap;
use std::error;
use std::fmt::Display;
use std::io;
use std::path::Path;
use std::time::SystemTime;

pub use aws_credential_types::Credentials;
use once_cell::sync::OnceCell;
use ureq::Response;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    SerdeJson(serde_json::Error),
    Request(Box<ureq::Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::SerdeJson(e) => write!(f, "JSON error: {}", e),
            Self::Request(e) => write!(f, "HTTP request error: {}", e),
        }
    }
}

impl error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeJson(e)
    }
}

impl From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        Self::Request(Box::new(e))
    }
}

pub struct Imds {
    token: OnceCell<String>,
    endpoint: String,
}

impl Default for Imds {
    fn default() -> Self {
        Self {
            token: OnceCell::new(),
            endpoint: "http://169.254.169.254".into(),
        }
    }
}

impl Imds {
    pub fn get(&self, path: &Path) -> Result<Response> {
        let token_url = format!("{}/latest/api/token", self.endpoint);
        let token = self.token.get_or_try_init(|| {
            ureq::put(&token_url)
                .set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                .call()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?
                .into_string()
        })?;
        let path_str = path.to_string_lossy();
        let url = format!("{}/{}", self.endpoint, path_str);
        ureq::get(&url)
            .set("X-aws-ec2-metadata-token", token)
            .call()
            .map_err(From::from)
    }

    pub fn get_user_data(&self) -> Result<String> {
        self.get(Path::new("latest/user-data"))
            .and_then(|response| response.into_string().map_err(From::from))
    }

    pub fn get_region(&self) -> Result<String> {
        self.get_metadata(Path::new("placement/region"))
    }

    pub fn get_metadata(&self, path: &Path) -> Result<String> {
        let full_path = Path::new("latest/meta-data").join(path);
        self.get(&full_path)
            .and_then(|response| response.into_string().map_err(From::from))
    }

    pub fn get_credentials(&self) -> Result<Credentials> {
        let role_path = Path::new("iam/security-credentials/");
        let role = self.get_metadata(role_path)?;
        let credentials_path = role_path.join(&role);
        let credentials_str = self.get_metadata(&credentials_path)?;
        let map: HashMap<String, String> = serde_json::from_str(&credentials_str)?;
        Credentials::from_map(map)
    }
}

trait CredentialsExt {
    fn from_map(map: HashMap<String, String>) -> Result<Credentials>;
}

impl CredentialsExt for Credentials {
    fn from_map(map: HashMap<String, String>) -> Result<Credentials> {
        let access_key_id = map
            .get("AccessKeyId")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "AccessKeyId not found"))?;
        let secret_access_key = map
            .get("SecretAccessKey")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "SecretAccessKey not found"))?;
        let session_token = map.get("Token").cloned();
        let expiration_str = map.get("Expiration");
        let expires_after = if let Some(e) = expiration_str {
            let parsed = chrono::DateTime::parse_from_rfc3339(e).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Unable to parse expiration: {}", e),
                )
            })?;
            Some(SystemTime::from(parsed))
        } else {
            None
        };
        Ok(Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            expires_after,
            "imds",
        ))
    }
}
