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

    pub fn rules_iter(&self) -> impl Iterator<Item = (&HostUpdate, &Rule)> {
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

    pub fn check_rule(&self, _rule: &Rule) -> Result<(), shared::Error> {
        todo!()
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
