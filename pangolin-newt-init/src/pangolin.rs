use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Site {
    pub site_id: u32,
    pub name: String,
    pub nice_id: String,
    pub status: Status,
    pub online: bool,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Approved,
    Pending,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    total: u32,
    page_size: u32,
}

#[derive(Deserialize, Debug)]
struct SitesData {
    sites: Vec<Site>,
    pagination: Pagination,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewtDetails {
    exit_node_id: u32,
    address: String,
    public_key: String,
    name: String,
    listen_port: u32,
    endpoint: String,
    subnet: String,
    client_address: String,
    newt_id: String,
    newt_secret: String,
}

impl NewtDetails {
    pub fn newt_id(&self) -> &String {
        &self.newt_id
    }
    pub fn newt_secret(&self) -> &String {
        &self.newt_secret
    }
}

#[derive(Deserialize, Debug)]
struct ApiResponse<T> {
    data: T,
}

pub struct PangolinClient {
    client: Client,
    base_url: String,
    token: String,
    org: String,
}

impl PangolinClient {
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        org: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into(),
            token: token.into(),
            org: org.into(),
        }
    }

    pub async fn find_site_by_name(
        &self,
        target_name: &str,
    ) -> Result<Option<Site>, shared::Error> {
        let mut page = 1u32;

        loop {
            let response = self
                .client
                .get(format!("{}/v1/org/{}/sites", self.base_url, self.org))
                .query(&[("page", page), ("pageSize", 20)])
                .header("Authorization", format!("Bearer {}", self.token))
                .send()
                .await?
                .json::<ApiResponse<SitesData>>()
                .await?;

            let data = response.data;

            if let Some(site) = data.sites.into_iter().find(|s| s.name == target_name) {
                return Ok(Some(site));
            }

            let total_pages = data.pagination.total.div_ceil(data.pagination.page_size);
            if page >= total_pages {
                break;
            }

            page += 1;
        }

        Ok(None)
    }

    pub async fn create_newt_credentials(&self) -> Result<NewtDetails, shared::Error> {
        let response = self
            .client
            .get(format!(
                "{}/v1/org/{}/pick-site-defaults",
                self.base_url, self.org
            ))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?
            .json::<ApiResponse<NewtDetails>>()
            .await?;

        Ok(response.data)
    }
}
