use std::collections::HashMap;

use log::error;
use moonlight_common::mac::MacAddress;
use pem::Pem;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::app::user::RoleType;

// Those version don't follow the release tags and are just arbitrary

#[derive(Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum Json {
    #[serde(rename = "4")]
    V4(V4),
    #[serde(rename = "3")]
    V3(V3),
    #[serde(rename = "2")]
    V2(V2),
    #[serde(untagged)]
    V1(V1),
}

// -- V1

#[derive(Serialize, Deserialize)]
pub struct V1 {
    hosts: Vec<V1Host>,
}

#[derive(Serialize, Deserialize)]
pub struct V1Host {
    address: String,
    http_port: u16,
    #[serde(default)]
    cache: V1HostCache,
    paired: Option<V1HostPairInfo>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct V1HostCache {
    pub name: Option<String>,
    pub mac: Option<MacAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V1HostPairInfo {
    pub client_private_key: String,
    pub client_certificate: String,
    pub server_certificate: String,
}

fn migrate_certificates_v1_to_v2(v1: V1HostPairInfo) -> Option<V2HostPairInfo> {
    Some(V2HostPairInfo {
        client_private_key: v1.client_private_key.parse().ok()?,
        client_certificate: v1.client_certificate.parse().ok()?,
        server_certificate: v1.server_certificate.parse().ok()?,
    })
}

pub fn migrate_v1_to_v2(old: V1) -> V2 {
    let mut v2_hosts = HashMap::new();

    for (id, old_host) in old.hosts.into_iter().enumerate() {
        let v2_host = V2Host {
            owner: None,
            address: old_host.address,
            http_port: old_host.http_port,
            pair_info: old_host
                .paired
                .and_then(|v1| match migrate_certificates_v1_to_v2(v1) {
                    Some(value) => Some(value),
                    None => {
                        error!("Migrating old pair data failed! Discarding this data!");
                        None
                    }
                }),
            cache: V2HostCache {
                name: old_host.cache.name.unwrap_or_else(|| "Unknown".to_string()),
                mac: old_host.cache.mac,
            },
        };

        v2_hosts.insert(id as u32, v2_host);
    }

    V2 {
        users: Default::default(),
        hosts: v2_hosts,
    }
}

// -- V2

use crate::app::storage::json::serde_helpers::{de_int_key, hex_array};

#[derive(Serialize, Deserialize)]
pub struct V2 {
    #[serde(deserialize_with = "de_int_key")]
    pub users: HashMap<u32, V2User>,
    #[serde(deserialize_with = "de_int_key")]
    pub hosts: HashMap<u32, V2Host>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2User {
    pub role: RoleType,
    pub name: String,
    pub password: Option<V2UserPassword>,
    pub client_unique_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2UserPassword {
    #[serde(with = "hex_array")]
    pub salt: [u8; 16],
    #[serde(with = "hex_array")]
    pub hash: [u8; 32],
    // Older storage files predate per-hash iteration tracking; default to the
    // value that was used at the time those hashes were created so they keep
    // verifying after the global iteration count is increased.
    #[serde(default = "default_legacy_iterations")]
    pub iterations: u32,
}

fn default_legacy_iterations() -> u32 {
    150_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2Host {
    pub owner: Option<u32>,
    pub address: String,
    pub http_port: u16,
    pub pair_info: Option<V2HostPairInfo>,
    pub cache: V2HostCache,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2HostPairInfo {
    pub client_private_key: Pem,
    pub client_certificate: Pem,
    pub server_certificate: Pem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V2HostCache {
    pub name: String,
    pub mac: Option<MacAddress>,
}

fn migrate_v2_to_v3(old: V2) -> V3 {
    const ADMIN_ID: u32 = 0;
    const USER_ID: u32 = 1;

    let mut roles = HashMap::new();

    roles.insert(
        ADMIN_ID,
        V3Role {
            name: "Admin".to_string(),
            ty: V3RoleType::Admin,
            default_settings: Default::default(),
            permissions: V3RolePermissions::default(),
        },
    );
    roles.insert(
        USER_ID,
        V3Role {
            name: "User".to_string(),
            ty: V3RoleType::User,
            default_settings: Default::default(),
            permissions: V3RolePermissions::default(),
        },
    );

    V3 {
        users: old
            .users
            .into_iter()
            .map(|(id, user)| {
                (
                    id,
                    V3User {
                        client_unique_id: user.client_unique_id,
                        name: user.name,
                        password: user.password,
                        settings: None,
                        role_id: match user.role {
                            RoleType::Admin => ADMIN_ID,
                            RoleType::User => USER_ID,
                        },
                    },
                )
            })
            .collect(),
        hosts: old.hosts,
        roles,
    }
}

// V3

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3 {
    #[serde(deserialize_with = "de_int_key")]
    pub users: HashMap<u32, V3User>,
    #[serde(deserialize_with = "de_int_key")]
    pub hosts: HashMap<u32, V2Host>,
    #[serde(deserialize_with = "de_int_key")]
    pub roles: HashMap<u32, V3Role>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3User {
    pub role_id: u32,
    pub name: String,
    pub password: Option<V2UserPassword>,
    pub client_unique_id: String,
    #[serde(default)]
    pub settings: Option<Value>,
}

// V4 gives per-user settings their own storage version. V3 intentionally keeps
// accepting the short-lived settings field so installations that already wrote
// it before V4 was introduced migrate without losing those values.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4 {
    #[serde(deserialize_with = "de_int_key")]
    pub users: HashMap<u32, V4User>,
    #[serde(deserialize_with = "de_int_key")]
    pub hosts: HashMap<u32, V2Host>,
    #[serde(deserialize_with = "de_int_key")]
    pub roles: HashMap<u32, V3Role>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4User {
    pub role_id: u32,
    pub name: String,
    pub password: Option<V2UserPassword>,
    pub client_unique_id: String,
    pub settings: Option<Value>,
    #[serde(default)]
    pub settings_revision: u64,
    #[serde(default)]
    pub settings_mutation_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum V3RoleType {
    User,
    Admin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3Role {
    pub name: String,
    pub ty: V3RoleType,
    pub default_settings: Value,
    pub permissions: V3RolePermissions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V3RolePermissions {
    pub allow_add_hosts: bool,
    pub maximum_bitrate_kbps: Option<u32>,
    pub allow_codec_h264: bool,
    pub allow_codec_h265: bool,
    pub allow_codec_av1: bool,
    pub allow_hdr: bool,
    pub allow_transport_webrtc: bool,
    pub allow_transport_websockets: bool,
}

impl Default for V3RolePermissions {
    fn default() -> Self {
        V3RolePermissions {
            allow_add_hosts: true,
            maximum_bitrate_kbps: None,
            allow_codec_h264: true,
            allow_codec_h265: true,
            allow_codec_av1: true,
            allow_hdr: true,
            allow_transport_webrtc: true,
            allow_transport_websockets: true,
        }
    }
}

fn migrate_v3_to_v4(old: V3) -> V4 {
    V4 {
        users: old
            .users
            .into_iter()
            .map(|(id, user)| {
                (
                    id,
                    V4User {
                        role_id: user.role_id,
                        name: user.name,
                        password: user.password,
                        client_unique_id: user.client_unique_id,
                        settings: user.settings,
                        settings_revision: 0,
                        settings_mutation_ids: Vec::new(),
                    },
                )
            })
            .collect(),
        hosts: old.hosts,
        roles: old.roles,
    }
}

pub fn migrate_to_latest(json: Json) -> Result<V4, anyhow::Error> {
    match json {
        Json::V1(v1) => Ok(migrate_v3_to_v4(migrate_v2_to_v3(migrate_v1_to_v2(v1)))),
        Json::V2(v2) => Ok(migrate_v3_to_v4(migrate_v2_to_v3(v2))),
        Json::V3(v3) => Ok(migrate_v3_to_v4(v3)),
        Json::V4(v4) => Ok(v4),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Json, V3User, V4User, migrate_to_latest};

    #[test]
    fn v3_user_without_settings_deserializes_with_none() {
        let user: V3User = serde_json::from_value(json!({
            "role_id": 7,
            "name": "legacy-user",
            "password": null,
            "client_unique_id": "legacy-client"
        }))
        .expect("an existing V3 user without settings should still deserialize");

        assert!(user.settings.is_none());
    }

    #[test]
    fn v3_with_interim_settings_migrates_to_v4_without_loss() {
        let json: Json = serde_json::from_value(json!({
            "version": "3",
            "users": {
                "7": {
                    "role_id": 3,
                    "name": "interim-user",
                    "password": null,
                    "client_unique_id": "interim-client",
                    "settings": { "bitrate": 17_000 }
                }
            },
            "hosts": {},
            "roles": {}
        }))
        .expect("interim V3 settings should deserialize");

        let v4 = migrate_to_latest(json).expect("V3 should migrate to V4");
        assert_eq!(
            v4.users
                .get(&7)
                .expect("migrated user should exist")
                .settings,
            Some(json!({ "bitrate": 17_000 }))
        );
        assert_eq!(
            v4.users
                .get(&7)
                .expect("migrated user should exist")
                .settings_revision,
            0
        );
        assert!(
            v4.users
                .get(&7)
                .expect("migrated user should exist")
                .settings_mutation_ids
                .is_empty()
        );
    }

    #[test]
    fn v4_user_without_revision_deserializes_with_zero() {
        let user: V4User = serde_json::from_value(json!({
            "role_id": 7,
            "name": "pre-revision-user",
            "password": null,
            "client_unique_id": "pre-revision-client",
            "settings": { "bitrate": 21_000 }
        }))
        .expect("an existing V4 user without a revision should still deserialize");

        assert_eq!(user.settings_revision, 0);
        assert_eq!(user.settings, Some(json!({ "bitrate": 21_000 })));
        assert!(user.settings_mutation_ids.is_empty());
    }
}
