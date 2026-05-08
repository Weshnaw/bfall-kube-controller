#![allow(dead_code)]
use gateway_api::apis::experimental::httproutes::HttpRouteRulesMatchesPathType;

use crate::pangolin::PangolinResourceConfig;

pub struct RetrievedData {
    hostnames: Vec<HostUpdate>,
    rules: Vec<Rule>,
}

impl RetrievedData {
    pub fn new(hostnames: Vec<HostUpdate>, rules: Vec<Rule>) -> Self {
        Self { hostnames, rules }
    }

    pub fn hosts_iter(&self) -> impl Iterator<Item = &HostUpdate> {
        self.hostnames.iter()
    }
    pub fn rules_iter(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    pub fn combined_iter(&self) -> impl Iterator<Item = (&HostUpdate, &Rule)> {
        self.hostnames
            .iter()
            .flat_map(|hostname| self.rules.iter().map(move |rule| (hostname, rule)))
    }
}

pub struct HostUpdate {
    host: String,
    pangolin_server: PangolinResourceConfig,
}

impl HostUpdate {
    pub fn new(host: String, pangolin_server: PangolinResourceConfig) -> Self {
        Self {
            host,
            pangolin_server,
        }
    }

    pub fn pangolin_server(&self) -> &PangolinResourceConfig {
        &self.pangolin_server
    }

    pub async fn check_hostname(&self) -> Result<(), shared::Error> {
        let client = self.pangolin_server.create_client();

        match self.pangolin_server.visibility() {
            crate::pangolin::Visibility::Public => {
                if client.check_host(&self.host).await? {
                    // TODO: consider somehow handling existing domains
                    return Err(shared::Error::Validate(
                        shared::ValidateError::DomainAlreadyInUse,
                    ));
                }

                Ok(())
            }
            crate::pangolin::Visibility::Private => Err(shared::Error::NotImplemented),
        }
    }

    pub fn apply_rule(&self, _rule: &Rule) -> Result<(), shared::Error> {
        todo!()
    }
}

pub struct Rule {
    backends: Vec<Backend>,
    _matches: Vec<Match>,
}

impl Rule {
    pub fn new(backends: Vec<Backend>, _matches: Vec<Match>) -> Self {
        Self { backends, _matches }
    }

    pub async fn check_rule(&self) -> Result<(), shared::Error> {
        // TODO: check if backend is up and working
        // TODO: check if matches are valid for pangolin
        Ok(())
    }
}

pub struct Backend {
    fqdn: String,
    port: i32,
}

impl Backend {
    pub fn new(fqdn: String, port: i32) -> Self {
        Self { fqdn, port }
    }
}

pub struct Match {
    path_type: HttpRouteRulesMatchesPathType,
    path: String,
}

impl Match {
    pub fn new(path_type: HttpRouteRulesMatchesPathType, path: String) -> Self {
        Self { path_type, path }
    }
}
