#![allow(dead_code)]

#[derive(Debug, Clone)]
pub struct PangolinApiConfig {
    api_endpoint: String,
    api_key: String,
    org: String,
    visibility: Visibility,
    sites: Vec<String>,
    listeners: Vec<Listener>,
}

impl PangolinApiConfig {
    pub fn new(
        api_endpoint: String,
        api_key: String,
        org: String,
        visibility: Visibility,
        sites: Vec<String>,
        listeners: Vec<Listener>,
    ) -> Self {
        PangolinApiConfig {
            api_endpoint,
            org,
            visibility,
            sites,
            api_key,
            listeners,
        }
    }

    pub fn listeners(&self) -> &Vec<Listener> {
        &self.listeners
    }
}

#[derive(Debug, Clone)]
pub struct Listener {
    port: i32,
    protocol: Protocol,
    tld: String,
    wildcard: bool,
}

impl Listener {
    pub fn new(port: i32, protocol: Protocol, tld: String, wildcard: bool) -> Self {
        Self {
            port,
            protocol,
            tld,
            wildcard,
        }
    }

    pub fn is_valid_domain(&self, hostname: impl AsRef<str>) -> bool {
        if self.wildcard {
            hostname.as_ref().ends_with(&format!(".{}", self.tld))
        } else {
            &self.tld == hostname.as_ref()
        }
    }
}

#[derive(Debug, Clone)]
pub enum Protocol {
    Https,
    Http, // Disables the SSL on pangolin for this address
}

impl Protocol {
    pub fn from_str(label: impl AsRef<str>) -> Self {
        match label.as_ref().to_ascii_lowercase().as_str() {
            "http" => Self::Http,
            _ => Self::Https, // TODO: should probably do proper invalid checks here
        }
    }
}

#[derive(Debug, Clone)]
pub enum Visibility {
    Public,
    Private,
}

impl Visibility {
    pub fn from_str(label: impl AsRef<str>) -> Option<Self> {
        match label.as_ref().to_ascii_lowercase().as_str() {
            "public" => Some(Self::Public),
            "private" => Some(Self::Private),
            _ => None,
        }
    }
}
