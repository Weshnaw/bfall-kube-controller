use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Site {
    site_id: u32,
    name: String,
    nice_id: String,
    // status: Status,
    online: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    newt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret: Option<String>,
}

impl Site {
    pub fn id(&self) -> u32 {
        self.site_id
    }
    pub fn nice_id(&self) -> &String {
        &self.nice_id
    }
    pub fn online(&self) -> bool {
        self.online
    }
    pub fn newt_id(&self) -> &Option<String> {
        &self.newt_id
    }
    pub fn secret(&self) -> &Option<String> {
        &self.secret
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateSiteRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    nice_id: Option<String>,
    #[serde(rename = "type")]
    site_type: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Approved,
    Pending,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Domain {
    base_domain: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Resource {
    full_domain: String,
}

#[derive(Deserialize, Debug)]
struct PaginatedData<T> {
    #[serde(alias = "domains")]
    #[serde(alias = "resources")]
    items: Vec<T>,
    pagination: Pagination,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Pagination {
    total: u32,
    #[serde(alias = "limit")]
    page_size: u32,
}

#[derive(Deserialize, Debug)]
struct ApiResponse<T> {
    data: T,
}

#[derive(Deserialize, Debug)]
struct ApiResponseSkipData {
    success: bool,
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
        target_name: impl AsRef<str>,
    ) -> Result<Option<Site>, crate::Error> {
        let mut page = 1u32;

        loop {
            let response = self
                .client
                .get(format!("{}/v1/org/{}/sites", self.base_url, self.org))
                .query(&[("page", page)])
                .header("Authorization", format!("Bearer {}", self.token))
                .send()
                .await?
                .json::<ApiResponse<PaginatedData<Site>>>()
                .await?;

            let data = response.data;

            if let Some(site) = data
                .items
                .into_iter()
                .find(|s| s.name == target_name.as_ref())
            {
                return Ok(Some(site));
            }

            // TODO: double check pagination math, this might check 1 more then needed
            let total_pages = data.pagination.total.div_ceil(data.pagination.page_size);
            if page >= total_pages {
                break;
            }

            page += 1;
        }

        Ok(None)
    }

    pub async fn delete_site(&self, site_id: u32) -> Result<(), crate::Error> {
        self.client
            .delete(format!("{}/v1/site/{}", self.base_url, site_id))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?;

        Ok(())
    }

    pub async fn create_site(
        &self,
        name: impl Into<String>,
        nice_id: Option<String>,
    ) -> Result<Site, crate::Error> {
        let body = CreateSiteRequest {
            name: name.into(),
            site_type: "newt".into(),
            nice_id,
        };

        let response = self
            .client
            .put(format!("{}/v1/org/{}/site", self.base_url, self.org))
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&body)
            .send()
            .await?
            .json::<ApiResponse<Site>>()
            .await?;

        Ok(response.data)
    }

    pub async fn domain_exists(&self, target: impl AsRef<str>) -> Result<bool, crate::Error> {
        let mut page = 0u32;

        loop {
            let response = self
                .client
                .get(format!("{}/v1/org/{}/domains", self.base_url, self.org))
                .query(&[("offset", page)])
                .header("Authorization", format!("Bearer {}", self.token))
                .send()
                .await?
                .json::<ApiResponse<PaginatedData<Domain>>>()
                .await?;

            if let Some(domain) = response
                .data
                .items
                .iter()
                .find(|s| s.base_domain == target.as_ref())
            {
                debug!(domain = ?domain, target = target.as_ref(), "Domain found...");
                return Ok(true);
            }

            trace!(response = ?response, "Domain not found on page");

            let total_pages = response
                .data
                .pagination
                .total
                .div_ceil(response.data.pagination.page_size);
            page += 1;
            if page >= total_pages {
                break;
            }
        }

        debug!(target = target.as_ref(), "Domain not found...");
        Ok(false)
    }

    pub async fn check_org(&self) -> Result<bool, crate::Error> {
        let response = self
            .client
            .get(format!("{}/v1/org/{}", self.base_url, self.org))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?
            .json::<ApiResponseSkipData>()
            .await?;

        Ok(response.success)
    }

    pub async fn check_site(&self, site_id: impl AsRef<str>) -> Result<bool, crate::Error> {
        let response = self
            .client
            .get(format!(
                "{}/v1/org/{}/site/{}",
                self.base_url,
                self.org,
                site_id.as_ref()
            ))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?
            .json::<ApiResponseSkipData>()
            .await?;

        Ok(response.success)
    }

    pub async fn check_host(&self, host_url: impl AsRef<str>) -> Result<bool, crate::Error> {
        let mut page = 1u32;

        loop {
            let response = self
                .client
                .get(format!("{}/v1/org/{}/resources", self.base_url, self.org))
                .query(&[("page", page)])
                .header("Authorization", format!("Bearer {}", self.token))
                .send()
                .await?
                .json::<ApiResponse<PaginatedData<Resource>>>()
                .await?;

            let data = response.data;

            if data
                .items
                .iter()
                .any(|s| s.full_domain == host_url.as_ref())
            {
                return Ok(false);
            }

            // TODO: double check pagination math, this might check 1 more then needed
            let total_pages = data.pagination.total.div_ceil(data.pagination.page_size);
            if page >= total_pages {
                break;
            }

            page += 1;
        }

        Ok(true)
    }
}
