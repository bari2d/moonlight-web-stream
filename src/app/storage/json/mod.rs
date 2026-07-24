use std::{
    collections::HashMap,
    ffi::OsString,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::join_all;
use openssl::rand::rand_bytes;
use serde_json::Value;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    spawn,
    sync::{
        RwLock,
        mpsc::{self, Receiver, Sender, error::TrySendError},
        oneshot,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error};

#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, ReplaceFileW,
};

use crate::app::{
    AppError,
    auth::SessionToken,
    host::HostId,
    password::StoragePassword,
    role::RoleId,
    storage::{
        Either, Storage, StorageHost, StorageHostAdd, StorageHostCache, StorageHostModify,
        StorageHostPairInfo, StorageQueryHosts, StorageRole, StorageRoleAdd,
        StorageRoleDefaultSettings, StorageRoleModify, StorageRolePermissions,
        StorageSettingsMutation, StorageUser, StorageUserAdd, StorageUserModify,
        json::versions::{
            Json, V2, V2Host, V2HostCache, V2HostPairInfo, V2UserPassword, V3Role,
            V3RolePermissions, V3RoleType, V4, V4User, migrate_to_latest,
        },
    },
    user::{RoleType, UserId},
};

mod serde_helpers;
mod versions;

const MAX_RECENT_SETTINGS_MUTATIONS: usize = 32;

pub struct JsonStorage {
    file: PathBuf,
    store_sender: Sender<StoreRequest>,
    session_expiration_checker: JoinHandle<()>,
    // IMPORTANT: only lock those mutexes in descending order to prevent deadlocks
    users: RwLock<HashMap<u32, RwLock<V4User>>>,
    hosts: RwLock<HashMap<u32, RwLock<V2Host>>>,
    roles: RwLock<HashMap<u32, RwLock<V3Role>>>,
    sessions: RwLock<HashMap<SessionToken, Session>>,
}

enum StoreRequest {
    Eventual,
    Durable(oneshot::Sender<Result<(), AppError>>),
}

impl Drop for JsonStorage {
    fn drop(&mut self) {
        self.session_expiration_checker.abort();
    }
}

struct Session {
    created_at: Instant,
    expiration: Duration,
    user_id: u32,
}

impl JsonStorage {
    pub async fn load(
        file: PathBuf,
        session_expiration_check_interval: Duration,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let (store_sender, store_receiver) = mpsc::channel(1);

        let (this_sender, this_receiver) = oneshot::channel::<Arc<Self>>();

        let session_expiration_checker = spawn(async move {
            let this = match this_receiver.await {
                Ok(value) => value,
                Err(err) => {
                    error!(
                        "Failed to initialize session expiration checker: {err:?}. All sessions will last forever!"
                    );
                    return;
                }
            };

            loop {
                sleep(session_expiration_check_interval).await;
                debug!("Clearing all expired sessions!");

                let mut sessions = this.sessions.write().await;

                let now = Instant::now();
                sessions.retain(|_, session| {
                    let current_session_length = now - session.created_at;

                    current_session_length < session.expiration
                });
            }
        });

        let this = Self {
            file,
            store_sender,
            session_expiration_checker,
            hosts: Default::default(),
            users: Default::default(),
            roles: Default::default(),
            sessions: Default::default(),
        };
        let this = Arc::new(this);

        if this_sender.send(this.clone()).is_err() {
            error!(
                "Failed to send values to session expiration checker. All sessions will last forever!"
            );
        }

        this.load_internal().await?;

        spawn({
            let this = this.clone();

            async move { file_writer(store_receiver, this).await }
        });

        Ok(this)
    }

    fn force_write(&self) {
        match self.store_sender.try_send(StoreRequest::Eventual) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Closed(_)) => {
                error!("Failed to save data because the writer task closed!");
            }
        }
    }

    async fn force_write_durable(&self) -> Result<(), AppError> {
        let (completion_sender, completion_receiver) = oneshot::channel();

        self.store_sender
            .send(StoreRequest::Durable(completion_sender))
            .await
            .map_err(|_| {
                io::Error::new(
                    ErrorKind::BrokenPipe,
                    "the JSON storage writer task closed before accepting a durable write",
                )
            })?;

        completion_receiver.await.map_err(|_| {
            AppError::Io(io::Error::new(
                ErrorKind::BrokenPipe,
                "the JSON storage writer task closed before acknowledging a durable write",
            ))
        })?
    }

    async fn load_internal(&self) -> Result<(), anyhow::Error> {
        let text = match fs::read_to_string(&self.file).await {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(());
            }
            Err(err) => {
                return Err(anyhow!("Failed to read data: {err:?}"));
            }
        };

        let json = match serde_json::from_str::<Json>(&text) {
            Ok(value) => value,
            Err(err) => {
                let error = serde_json::from_str::<V2>(&text)
                    .err()
                    .map(|x| x.to_string())
                    .unwrap_or("none".to_string());

                return Err(anyhow!(
                    "Failed to deserialize data as json: {err}, Version specific error: {error}"
                ));
            }
        };

        let data = migrate_to_latest(json)?;

        {
            let mut users = self.users.write().await;
            let mut hosts = self.hosts.write().await;
            let mut roles = self.roles.write().await;

            *users = data
                .users
                .into_iter()
                .map(|(id, user)| (id, RwLock::new(user)))
                .collect();
            *hosts = data
                .hosts
                .into_iter()
                .map(|(id, host)| (id, RwLock::new(host)))
                .collect();
            *roles = data
                .roles
                .into_iter()
                .map(|(id, role)| (id, RwLock::new(role)))
                .collect();
        }

        Ok(())
    }
    async fn store(&self) -> Result<(), AppError> {
        let json = {
            let users = self.users.read().await;
            let hosts = self.hosts.read().await;
            let roles = self.roles.read().await;

            let mut users_json = HashMap::new();
            for (key, value) in users.iter() {
                let value = value.read().await;

                users_json.insert(*key, (*value).clone());
            }

            let mut hosts_json = HashMap::new();
            for (key, value) in hosts.iter() {
                let value = value.read().await;

                hosts_json.insert(*key, (*value).clone());
            }

            let mut roles_json = HashMap::new();
            for (key, value) in roles.iter() {
                let value = value.read().await;

                roles_json.insert(*key, (*value).clone());
            }

            Json::V4(V4 {
                users: users_json,
                hosts: hosts_json,
                roles: roles_json,
            })
        };

        let text = serde_json::to_string_pretty(&json).map_err(|err| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("failed to serialize storage data as JSON: {err}"),
            )
        })?;

        atomic_write(&self.file, text.as_bytes()).await?;

        Ok(())
    }
}

async fn write_synced_temp(destination: &Path, contents: &[u8]) -> Result<PathBuf, AppError> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = destination.file_name().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "the JSON storage path has no filename",
        )
    })?;

    #[cfg(unix)]
    let destination_permissions = {
        use std::os::unix::fs::PermissionsExt;

        match fs::metadata(destination).await {
            Ok(metadata) => metadata.permissions(),
            Err(err) if err.kind() == ErrorKind::NotFound => std::fs::Permissions::from_mode(0o600),
            Err(err) => return Err(err.into()),
        }
    };

    let (temp_path, mut temp_file) = {
        let mut opened = None;
        for _ in 0..16 {
            let mut temp_name = OsString::from(".");
            temp_name.push(file_name);
            temp_name.push(format!(".{}.tmp", random_number()?));
            let temp_path = parent.join(temp_name);

            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);

            match options.open(&temp_path).await {
                Ok(file) => {
                    opened = Some((temp_path, file));
                    break;
                }
                Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        }

        opened.ok_or_else(|| {
            AppError::Io(io::Error::new(
                ErrorKind::AlreadyExists,
                "could not allocate a unique temporary JSON storage file",
            ))
        })?
    };

    let write_result = async {
        #[cfg(unix)]
        temp_file.set_permissions(destination_permissions).await?;
        temp_file.write_all(contents).await?;
        temp_file.flush().await?;
        temp_file.sync_all().await
    }
    .await;
    drop(temp_file);

    if let Err(err) = write_result {
        remove_temp_file(&temp_path).await;
        return Err(err.into());
    }

    Ok(temp_path)
}

async fn remove_temp_file(path: &Path) {
    if let Err(err) = fs::remove_file(path).await
        && err.kind() != ErrorKind::NotFound
    {
        error!("Failed to remove temporary storage file {path:?}: {err}");
    }
}

async fn atomic_write(destination: &Path, contents: &[u8]) -> Result<(), AppError> {
    let temp_path = write_synced_temp(destination, contents).await?;

    if let Err(err) = atomic_replace(&temp_path, destination).await {
        remove_temp_file(&temp_path).await;
        return Err(err.into());
    }

    Ok(())
}

#[cfg(unix)]
async fn atomic_replace(temp_path: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(temp_path, destination).await?;

    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::File::open(parent).await?.sync_all().await
}

#[cfg(windows)]
async fn atomic_replace(temp_path: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    let destination_exists = match fs::metadata(destination).await {
        Ok(_) => true,
        Err(err) if err.kind() == ErrorKind::NotFound => false,
        Err(err) => return Err(err),
    };

    let destination_path = destination.to_path_buf();
    let temp_path = temp_path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();

    tokio::task::spawn_blocking(move || {
        // ReplaceFileW carries the existing file's ACL and other security
        // metadata forward. MoveFileExW is only used for the initial creation.
        let result = if destination_exists {
            unsafe {
                ReplaceFileW(
                    destination.as_ptr(),
                    temp_path.as_ptr(),
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    std::ptr::null(),
                )
            }
        } else {
            unsafe {
                MoveFileExW(
                    temp_path.as_ptr(),
                    destination.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            }
        };
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })
    .await
    .map_err(|err| io::Error::other(format!("atomic replacement task failed: {err}")))??;

    // REPLACEFILE_WRITE_THROUGH is not a supported ReplaceFileW flag. Flush
    // the replaced destination explicitly after the atomic swap instead.
    OpenOptions::new()
        .write(true)
        .open(destination_path)
        .await?
        .sync_all()
        .await
}

#[cfg(not(any(unix, windows)))]
async fn atomic_replace(temp_path: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(temp_path, destination).await
}

async fn file_writer(mut store_receiver: Receiver<StoreRequest>, json: Arc<JsonStorage>) {
    while let Some(request) = store_receiver.recv().await {
        match request {
            StoreRequest::Eventual => {
                if let Err(err) = json.store().await {
                    error!("Failed to save data: {err}");
                }
            }
            StoreRequest::Durable(completion) => {
                let _ = completion.send(json.store().await);
            }
        }
    }
}

fn permissions_from_json(permissions: V3RolePermissions) -> StorageRolePermissions {
    StorageRolePermissions {
        allow_add_hosts: permissions.allow_add_hosts,
        maximum_bitrate_kbps: permissions.maximum_bitrate_kbps,
        allow_codec_h264: permissions.allow_codec_h264,
        allow_codec_h265: permissions.allow_codec_h265,
        allow_codec_av1: permissions.allow_codec_av1,
        allow_hdr: permissions.allow_hdr,
        allow_transport_webrtc: permissions.allow_transport_webrtc,
        allow_transport_websockets: permissions.allow_transport_websockets,
    }
}
fn permissions_to_json(permissions: StorageRolePermissions) -> V3RolePermissions {
    V3RolePermissions {
        allow_add_hosts: permissions.allow_add_hosts,
        maximum_bitrate_kbps: permissions.maximum_bitrate_kbps,
        allow_codec_h264: permissions.allow_codec_h264,
        allow_codec_h265: permissions.allow_codec_h265,
        allow_codec_av1: permissions.allow_codec_av1,
        allow_hdr: permissions.allow_hdr,
        allow_transport_webrtc: permissions.allow_transport_webrtc,
        allow_transport_websockets: permissions.allow_transport_websockets,
    }
}

fn role_from_json(role_id: RoleId, role: &V3Role) -> StorageRole {
    StorageRole {
        id: role_id,
        name: role.name.clone(),
        ty: match role.ty {
            V3RoleType::Admin => RoleType::Admin,
            V3RoleType::User => RoleType::User,
        },
        default_settings: StorageRoleDefaultSettings {
            value: role.default_settings.clone(),
        },
        permissions: permissions_from_json(role.permissions.clone()),
    }
}

fn user_from_json(user_id: UserId, user: &V4User) -> StorageUser {
    StorageUser {
        id: user_id,
        name: user.name.clone(),
        password: user.password.as_ref().map(|password| StoragePassword {
            salt: password.salt,
            hash: password.hash,
            iterations: password.iterations,
        }),
        role_id: RoleId(user.role_id),
        client_unique_id: user.client_unique_id.clone(),
        settings: user.settings.clone(),
        settings_revision: user.settings_revision,
        settings_mutation_ids: user.settings_mutation_ids.clone(),
    }
}

fn apply_top_level_settings_patch(
    current: &mut Option<Value>,
    patch: Option<Value>,
) -> Result<(), AppError> {
    let Some(patch) = patch else {
        *current = None;
        return Ok(());
    };
    let Value::Object(patch) = patch else {
        return Err(AppError::BadRequest);
    };

    let mut merged = match current.take() {
        Some(Value::Object(settings)) => settings,
        _ => serde_json::Map::new(),
    };
    for (key, value) in patch {
        if value.is_null() {
            merged.remove(&key);
        } else {
            merged.insert(key, value);
        }
    }
    *current = Some(Value::Object(merged));

    Ok(())
}

fn host_from_json(host_id: HostId, host: &V2Host) -> StorageHost {
    StorageHost {
        id: host_id,
        owner: host.owner.map(UserId),
        address: host.address.clone(),
        http_port: host.http_port,
        pair_info: host.pair_info.clone().map(|pair_info| StorageHostPairInfo {
            client_certificate: pair_info.client_certificate,
            client_private_key: pair_info.client_private_key,
            server_certificate: pair_info.server_certificate,
        }),
        cache: StorageHostCache {
            name: host.cache.name.clone(),
            mac: host.cache.mac,
        },
    }
}

fn random_number() -> Result<u32, AppError> {
    let mut id_bytes = [0u8; 4];
    rand_bytes(&mut id_bytes)?;
    Ok(u32::from_be_bytes(id_bytes))
}

#[async_trait]
impl Storage for JsonStorage {
    async fn add_role(&self, role: StorageRoleAdd) -> Result<StorageRole, AppError> {
        let role = V3Role {
            ty: match role.ty {
                RoleType::Admin => V3RoleType::Admin,
                RoleType::User => V3RoleType::User,
            },
            name: role.name,
            default_settings: role.default_settings.value,
            permissions: permissions_to_json(role.permissions),
        };

        let mut roles = self.roles.write().await;

        let mut id;
        loop {
            id = random_number()?;

            if !roles.contains_key(&id) {
                break;
            }
        }
        roles.insert(id, RwLock::new(role.clone()));

        drop(roles);

        self.force_write();

        Ok(StorageRole {
            ty: match role.ty {
                V3RoleType::Admin => RoleType::Admin,
                V3RoleType::User => RoleType::User,
            },
            id: RoleId(id),
            name: role.name,
            default_settings: StorageRoleDefaultSettings {
                value: role.default_settings,
            },
            permissions: permissions_from_json(role.permissions),
        })
    }
    async fn modify_role(
        &self,
        role_id: RoleId,
        modify: StorageRoleModify,
    ) -> Result<(), AppError> {
        let roles = self.roles.read().await;

        let role_lock = roles.get(&role_id.0).ok_or(AppError::RoleNotFound)?;
        let mut role = role_lock.write().await;

        if let Some(name) = modify.name {
            role.name = name;
        }
        if let Some(ty) = modify.ty {
            role.ty = match ty {
                RoleType::Admin => V3RoleType::Admin,
                RoleType::User => V3RoleType::User,
            };
        }
        if let Some(StorageRoleDefaultSettings { value }) = modify.default_settings {
            role.default_settings = value;
        }
        if let Some(StorageRolePermissions {
            allow_add_hosts,
            maximum_bitrate_kbps,
            allow_codec_h264,
            allow_codec_h265,
            allow_codec_av1,
            allow_hdr,
            allow_transport_webrtc,
            allow_transport_websockets,
        }) = modify.permissions
        {
            role.permissions.allow_add_hosts = allow_add_hosts;
            role.permissions.maximum_bitrate_kbps = maximum_bitrate_kbps;
            role.permissions.allow_codec_h264 = allow_codec_h264;
            role.permissions.allow_codec_h265 = allow_codec_h265;
            role.permissions.allow_codec_av1 = allow_codec_av1;
            role.permissions.allow_hdr = allow_hdr;
            role.permissions.allow_transport_webrtc = allow_transport_webrtc;
            role.permissions.allow_transport_websockets = allow_transport_websockets;
        }

        drop(role);
        drop(roles);

        self.force_write();

        Ok(())
    }
    async fn get_role(&self, role_id: RoleId) -> Result<StorageRole, AppError> {
        let roles = self.roles.read().await;

        let role_lock = roles.get(&role_id.0).ok_or(AppError::RoleNotFound)?;
        let role = role_lock.read().await;

        Ok(role_from_json(role_id, &role))
    }
    async fn remove_role(&self, role_id: RoleId) -> Result<(), AppError> {
        // Delete all users with that role
        let users_to_remove = {
            let mut users = self.users.write().await;

            let mut users_to_remove = vec![];

            // Find all users with that role
            for (user_id, user) in users.iter() {
                let user = user.read().await;

                if user.role_id == role_id.0 {
                    users_to_remove.push(*user_id);
                }
            }

            // Remove all user id's in that list
            for user_id in &users_to_remove {
                users.remove(user_id);
            }

            users_to_remove
        };

        for user_id in users_to_remove {
            self.remove_all_user_session_tokens(UserId(user_id)).await?;
        }

        // Delete that role
        let result = {
            let mut roles = self.roles.write().await;

            let result = match roles.remove(&role_id.0) {
                None => Err(AppError::RoleNotFound),
                Some(_) => Ok(()),
            };

            drop(roles);

            result
        };

        self.force_write();

        result
    }
    async fn list_roles(&self) -> Result<Either<Vec<RoleId>, Vec<StorageRole>>, AppError> {
        let roles = self.roles.read().await;

        let futures = roles.iter().map(|(id, value)| {
            let id = *id;
            async move {
                let role = value.read().await.clone();
                role_from_json(RoleId(id), &role)
            }
        });

        let out = join_all(futures).await;
        Ok(Either::Right(out))
    }

    async fn add_user(&self, user: StorageUserAdd) -> Result<StorageUser, AppError> {
        let user = V4User {
            role_id: user.role_id.0,
            name: user.name,
            password: user.password.map(|password| V2UserPassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            }),
            client_unique_id: user.client_unique_id,
            settings: None,
            settings_revision: 0,
            settings_mutation_ids: Vec::new(),
        };

        {
            match self.get_user_by_name(&user.name).await {
                Err(AppError::UserNotFound) => {
                    // Fallthrough
                }
                Ok(_) => return Err(AppError::UserAlreadyExists),
                Err(err) => return Err(err),
            }
        }

        let mut users = self.users.write().await;

        let mut id;
        loop {
            id = random_number()?;

            if !users.contains_key(&id) {
                break;
            }
        }
        users.insert(id, RwLock::new(user.clone()));

        drop(users);

        self.force_write();

        Ok(StorageUser {
            id: UserId(id),
            name: user.name,
            password: user.password.map(|password| StoragePassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            }),
            role_id: RoleId(user.role_id),
            client_unique_id: user.client_unique_id,
            settings: user.settings,
            settings_revision: user.settings_revision,
            settings_mutation_ids: user.settings_mutation_ids,
        })
    }
    async fn modify_user(
        &self,
        user_id: UserId,
        modify: StorageUserModify,
    ) -> Result<(), AppError> {
        let users = self.users.read().await;

        let user_lock = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        let mut user = user_lock.write().await;

        if let Some(password) = modify.password {
            user.password = password.map(|password| V2UserPassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            });
        }
        if let Some(role_id) = modify.role_id {
            user.role_id = role_id.0;
        }
        if let Some(client_unique_id) = modify.client_unique_id {
            user.client_unique_id = client_unique_id;
        }

        drop(user);
        drop(users);

        self.force_write();

        Ok(())
    }
    async fn modify_user_settings(
        &self,
        user_id: UserId,
        settings_patch: Option<Value>,
        mutation_id: String,
    ) -> Result<StorageSettingsMutation, AppError> {
        let users = self.users.read().await;
        let user_lock = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        let mut user = user_lock.write().await;
        if user
            .settings_mutation_ids
            .iter()
            .any(|applied_id| applied_id == &mutation_id)
        {
            let revision = user.settings_revision;
            drop(user);
            drop(users);

            // A previous attempt may have updated memory but failed its durable
            // write. A duplicate still waits for persistence before it is
            // acknowledged as successfully applied.
            self.force_write_durable().await?;
            return Ok(StorageSettingsMutation {
                revision,
                applied: false,
            });
        }
        let revision = user.settings_revision.checked_add(1).ok_or_else(|| {
            AppError::Io(io::Error::new(
                ErrorKind::InvalidData,
                "the user settings revision reached its maximum value",
            ))
        })?;

        apply_top_level_settings_patch(&mut user.settings, settings_patch)?;
        user.settings_revision = revision;
        user.settings_mutation_ids.push(mutation_id);
        if user.settings_mutation_ids.len() > MAX_RECENT_SETTINGS_MUTATIONS {
            let excess = user.settings_mutation_ids.len() - MAX_RECENT_SETTINGS_MUTATIONS;
            user.settings_mutation_ids.drain(..excess);
        }

        drop(user);
        drop(users);

        self.force_write_durable().await?;
        Ok(StorageSettingsMutation {
            revision,
            applied: true,
        })
    }
    async fn get_user(&self, user_id: UserId) -> Result<StorageUser, AppError> {
        let users = self.users.read().await;

        let user_lock = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        let user = user_lock.read().await;

        Ok(user_from_json(user_id, &user))
    }
    async fn get_user_by_name(
        &self,
        name: &str,
    ) -> Result<(UserId, Option<StorageUser>), AppError> {
        let users = self.users.read().await;

        let results = join_all(users.iter().map(|(user_id, user)| async move {
            let user = user.read().await;

            let user_id = UserId(*user_id);
            let user = (user.name == name).then(|| user_from_json(user_id, &user));

            (user_id, user)
        }))
        .await;

        let user = results.into_iter().find(|(_, user)| user.is_some());

        user.ok_or(AppError::UserNotFound)
    }
    async fn remove_user(&self, user_id: UserId) -> Result<(), AppError> {
        let mut users = self.users.write().await;

        if users.remove(&user_id.0).is_none() {
            return Err(AppError::UserNotFound);
        }

        drop(users);

        self.remove_all_user_session_tokens(user_id).await?;
        self.force_write();

        Ok(())
    }
    async fn list_users(&self) -> Result<Either<Vec<UserId>, Vec<StorageUser>>, AppError> {
        let users = self.users.read().await;

        let futures = users.iter().map(|(id, value)| {
            let id = *id;
            async move {
                let user = value.read().await.clone();
                user_from_json(UserId(id), &user)
            }
        });

        let out = join_all(futures).await;
        Ok(Either::Right(out))
    }
    async fn any_user_exists(&self) -> Result<bool, AppError> {
        let users = self.users.read().await;

        Ok(!users.is_empty())
    }

    async fn create_session_token(
        &self,
        user_id: UserId,
        expiration: Duration,
    ) -> Result<SessionToken, AppError> {
        let users = self.users.read().await;
        if !users.contains_key(&user_id.0) {
            return Err(AppError::UserNotFound);
        }

        let mut token;
        {
            let sessions = self.sessions.read().await;

            loop {
                token = SessionToken::new()?;
                if !sessions.contains_key(&token) {
                    break;
                }
            }
        };

        let mut sessions = self.sessions.write().await;

        sessions.insert(
            token,
            Session {
                created_at: Instant::now(),
                expiration,
                user_id: user_id.0,
            },
        );

        Ok(token)
    }
    async fn remove_session_token(&self, session: SessionToken) -> Result<(), AppError> {
        let mut sessions = self.sessions.write().await;

        sessions.remove(&session);

        Ok(())
    }
    async fn remove_all_user_session_tokens(&self, user_id: UserId) -> Result<(), AppError> {
        let mut sessions = self.sessions.write().await;

        sessions.retain(|_, session| UserId(session.user_id) != user_id);

        Ok(())
    }
    async fn get_user_by_session_token(
        &self,
        session: SessionToken,
    ) -> Result<(UserId, Option<StorageUser>), AppError> {
        let user_id = self
            .sessions
            .read()
            .await
            .get(&session)
            .map(|session| UserId(session.user_id))
            .ok_or(AppError::SessionTokenNotFound)?;

        match self.get_user(user_id).await {
            Ok(user) => Ok((user_id, Some(user))),
            Err(AppError::UserNotFound) => {
                self.remove_session_token(session).await?;
                Err(AppError::SessionTokenNotFound)
            }
            Err(err) => Err(err),
        }
    }

    async fn add_host(&self, host: StorageHostAdd) -> Result<StorageHost, AppError> {
        let host = V2Host {
            owner: host.owner.map(|user_id| user_id.0),
            address: host.address,
            http_port: host.http_port,
            pair_info: host.pair_info.map(|pair_info| V2HostPairInfo {
                client_private_key: pair_info.client_private_key,
                client_certificate: pair_info.client_certificate,
                server_certificate: pair_info.server_certificate,
            }),
            cache: V2HostCache {
                name: host.cache.name,
                mac: host.cache.mac,
            },
        };

        let mut hosts = self.hosts.write().await;

        let mut id;
        loop {
            id = random_number()?;

            if !hosts.contains_key(&id) {
                break;
            }
        }
        hosts.insert(id, RwLock::new(host.clone()));

        self.force_write();

        Ok(StorageHost {
            id: HostId(id),
            owner: host.owner.map(UserId),
            address: host.address,
            http_port: host.http_port,
            pair_info: host.pair_info.map(|pair_info| StorageHostPairInfo {
                client_private_key: pair_info.client_private_key,
                client_certificate: pair_info.client_certificate,
                server_certificate: pair_info.server_certificate,
            }),
            cache: StorageHostCache {
                name: host.cache.name,
                mac: host.cache.mac,
            },
        })
    }
    async fn modify_host(
        &self,
        host_id: HostId,
        modify: StorageHostModify,
    ) -> Result<(), AppError> {
        let hosts = self.hosts.read().await;

        let host = hosts.get(&host_id.0).ok_or(AppError::HostNotFound)?;
        let mut host = host.write().await;

        if let Some(new_owner) = modify.owner {
            host.owner = new_owner.map(|user_id| user_id.0);
        }
        if let Some(new_address) = modify.address {
            host.address = new_address;
        }
        if let Some(new_http_port) = modify.http_port {
            host.http_port = new_http_port;
        }
        if let Some(new_pair_info) = modify.pair_info {
            host.pair_info = new_pair_info.map(|new_pair_info| V2HostPairInfo {
                client_private_key: new_pair_info.client_private_key,
                client_certificate: new_pair_info.client_certificate,
                server_certificate: new_pair_info.server_certificate,
            });
        }
        if let Some(new_cache_name) = modify.cache_name {
            host.cache.name = new_cache_name;
        }
        if let Some(new_cache_mac) = modify.cache_mac {
            host.cache.mac = new_cache_mac;
        }

        self.force_write();

        Ok(())
    }
    async fn get_host(&self, host_id: HostId) -> Result<StorageHost, AppError> {
        let hosts = self.hosts.read().await;

        let host = hosts.get(&host_id.0).ok_or(AppError::HostNotFound)?;
        let host = host.read().await;

        Ok(host_from_json(host_id, &host))
    }
    async fn remove_host(&self, host_id: HostId) -> Result<(), AppError> {
        let mut hosts = self.hosts.write().await;

        if hosts.remove(&host_id.0).is_none() {
            return Err(AppError::HostNotFound);
        }

        self.force_write();

        Ok(())
    }

    async fn list_user_hosts(
        &self,
        query: StorageQueryHosts,
    ) -> Result<Vec<(HostId, Option<StorageHost>)>, AppError> {
        let hosts = self.hosts.read().await;

        let mut user_hosts = Vec::new();
        for (host_id, host) in &*hosts {
            let host_id = HostId(*host_id);
            let host = host.read().await;

            if host.owner.is_none() || host.owner.map(UserId) == Some(query.user_id) {
                user_hosts.push((host_id, Some(host_from_json(host_id, &host))));
            }
        }

        Ok(user_hosts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_storage() -> JsonStorage {
        let (store_sender, mut store_receiver) = mpsc::channel(1);
        let session_expiration_checker = spawn(async move {
            while let Some(request) = store_receiver.recv().await {
                if let StoreRequest::Durable(completion) = request {
                    let _ = completion.send(Ok(()));
                }
            }
        });

        JsonStorage {
            file: PathBuf::new(),
            store_sender,
            session_expiration_checker,
            users: Default::default(),
            hosts: Default::default(),
            roles: Default::default(),
            sessions: Default::default(),
        }
    }

    fn temporary_storage_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "moonlight-web-{label}-{}.json",
            random_number().expect("temporary filename should be generated")
        ))
    }

    async fn load_test_storage(path: PathBuf) -> Arc<JsonStorage> {
        JsonStorage::load(path, Duration::from_secs(3_600))
            .await
            .expect("test storage should load")
    }

    async fn temporary_files_for(destination: &Path) -> Vec<PathBuf> {
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let prefix = format!(
            ".{}.",
            destination
                .file_name()
                .expect("destination should have a filename")
                .to_string_lossy()
        );
        let mut entries = fs::read_dir(parent)
            .await
            .expect("temporary storage directory should be readable");
        let mut paths = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .expect("temporary storage entry should be readable")
        {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".tmp") {
                paths.push(entry.path());
            }
        }
        paths
    }

    #[tokio::test]
    async fn interrupted_temp_write_leaves_original_json_untouched() {
        let path = temporary_storage_path("atomic-interruption");
        let original = serde_json::json!({ "version": "original", "value": 1 });
        fs::write(
            &path,
            serde_json::to_vec(&original).expect("original JSON should serialize"),
        )
        .await
        .expect("original JSON should be written");

        let temp_path = write_synced_temp(&path, br#"{"incomplete": "#)
            .await
            .expect("interrupted temporary write should be staged");

        let still_original: serde_json::Value = serde_json::from_slice(
            &fs::read(&path)
                .await
                .expect("original JSON should remain readable"),
        )
        .expect("original JSON should remain valid");
        assert_eq!(still_original, original);

        remove_temp_file(&temp_path).await;
        fs::remove_file(path)
            .await
            .expect("original test file should be removed");
    }

    #[tokio::test]
    async fn atomic_replacement_cleans_temporary_file() {
        let path = temporary_storage_path("atomic-replacement");
        fs::write(&path, br#"{"value":"old"}"#)
            .await
            .expect("original JSON should be written");

        atomic_write(&path, br#"{"value":"new"}"#)
            .await
            .expect("atomic replacement should succeed");

        let replaced: serde_json::Value = serde_json::from_slice(
            &fs::read(&path)
                .await
                .expect("replaced JSON should be readable"),
        )
        .expect("replaced JSON should remain valid");
        assert_eq!(replaced, serde_json::json!({ "value": "new" }));
        assert!(temporary_files_for(&path).await.is_empty());

        fs::remove_file(path)
            .await
            .expect("replacement test file should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn atomic_replacement_preserves_existing_mode_and_new_files_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let existing = temporary_storage_path("atomic-permissions-existing");
        fs::write(&existing, br#"{"value":"old"}"#)
            .await
            .expect("existing JSON should be written");
        fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o600))
            .await
            .expect("existing JSON should be made private");

        atomic_write(&existing, br#"{"value":"new"}"#)
            .await
            .expect("existing JSON should be replaced");
        assert_eq!(
            fs::metadata(&existing)
                .await
                .expect("existing JSON metadata should be readable")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let new_file = temporary_storage_path("atomic-permissions-new");
        atomic_write(&new_file, br#"{"value":"new"}"#)
            .await
            .expect("new JSON should be created");
        assert_eq!(
            fs::metadata(&new_file)
                .await
                .expect("new JSON metadata should be readable")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        fs::remove_file(existing)
            .await
            .expect("existing permissions test file should be removed");
        fs::remove_file(new_file)
            .await
            .expect("new permissions test file should be removed");
    }

    #[tokio::test]
    async fn failed_atomic_replacement_cleans_temporary_file() {
        let parent = temporary_storage_path("atomic-failure").with_extension("");
        let destination = parent.join("data.json");
        fs::create_dir(&parent)
            .await
            .expect("test parent directory should be created");
        fs::create_dir(&destination)
            .await
            .expect("directory destination should be created");

        assert!(
            atomic_write(&destination, br#"{"value":"new"}"#)
                .await
                .is_err()
        );
        assert!(temporary_files_for(&destination).await.is_empty());

        fs::remove_dir(&destination)
            .await
            .expect("directory destination should be removed");
        fs::remove_dir(parent)
            .await
            .expect("test parent directory should be removed");
    }

    async fn add_named_test_user(
        storage: &JsonStorage,
        role_id: RoleId,
        name: &str,
    ) -> StorageUser {
        storage
            .add_user(StorageUserAdd {
                role_id,
                name: name.to_string(),
                password: None,
                client_unique_id: format!("{name}-client"),
            })
            .await
            .expect("test user should be created")
    }

    async fn add_test_user(storage: &JsonStorage, role_id: RoleId) -> StorageUser {
        add_named_test_user(storage, role_id, "test-user").await
    }

    #[tokio::test]
    async fn user_settings_persist_and_are_isolated() {
        let path = temporary_storage_path("user-settings");
        let storage = load_test_storage(path.clone()).await;
        let first_user = add_named_test_user(&storage, RoleId(1), "first-user").await;
        let second_user = add_named_test_user(&storage, RoleId(1), "second-user").await;

        assert!(first_user.settings.is_none());
        assert!(second_user.settings.is_none());
        assert_eq!(first_user.settings_revision, 0);
        assert_eq!(second_user.settings_revision, 0);
        assert!(first_user.settings_mutation_ids.is_empty());
        assert!(second_user.settings_mutation_ids.is_empty());

        let settings = serde_json::json!({
            "bitrate": 18_000,
            "resolution": { "width": 1920, "height": 1080 }
        });
        let first_mutation = storage
            .modify_user_settings(
                first_user.id,
                Some(settings.clone()),
                "first-settings".to_string(),
            )
            .await
            .expect("user settings should be updated");
        assert_eq!(
            first_mutation,
            StorageSettingsMutation {
                revision: 1,
                applied: true
            }
        );

        let stored_json: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&path)
                .await
                .expect("durable write should create the storage file"),
        )
        .expect("stored data should be valid JSON");
        assert_eq!(stored_json["version"], "4");
        assert_eq!(
            stored_json["users"][first_user.id.0.to_string()]["settings_revision"],
            1
        );

        let reloaded = load_test_storage(path.clone()).await;

        let reloaded_first = reloaded
            .get_user(first_user.id)
            .await
            .expect("first user should reload");
        assert_eq!(reloaded_first.settings, Some(settings));
        assert_eq!(reloaded_first.settings_revision, 1);
        assert_eq!(
            reloaded_first.settings_mutation_ids,
            vec!["first-settings".to_string()]
        );
        assert!(
            reloaded
                .get_user(second_user.id)
                .await
                .expect("second user should reload")
                .settings
                .is_none()
        );

        let clear_mutation = reloaded
            .modify_user_settings(first_user.id, None, "clear-settings".to_string())
            .await
            .expect("user settings should be cleared");
        assert_eq!(
            clear_mutation,
            StorageSettingsMutation {
                revision: 2,
                applied: true
            }
        );

        let cleared = load_test_storage(path.clone()).await;
        let cleared_user = cleared
            .get_user(first_user.id)
            .await
            .expect("first user should still exist");
        assert!(cleared_user.settings.is_none());
        assert_eq!(cleared_user.settings_revision, 2);

        fs::remove_file(path)
            .await
            .expect("temporary storage file should be removed");
    }

    #[tokio::test]
    async fn partial_settings_merge_is_idempotent_and_does_not_revert_later_mutations() {
        let path = temporary_storage_path("settings-idempotency");
        let storage = load_test_storage(path.clone()).await;
        let user = add_named_test_user(&storage, RoleId(1), "merge-user").await;
        let initial_patch = serde_json::json!({
            "bitrate": 18_000,
            "codec": "h264",
            "resolution": { "width": 1920, "height": 1080 }
        });

        let initial = storage
            .modify_user_settings(
                user.id,
                Some(initial_patch.clone()),
                "initial-patch".to_string(),
            )
            .await
            .expect("initial patch should be persisted");
        assert_eq!(
            initial,
            StorageSettingsMutation {
                revision: 1,
                applied: true
            }
        );

        let later = storage
            .modify_user_settings(
                user.id,
                Some(serde_json::json!({
                    "bitrate": 22_000,
                    "codec": null,
                    "resolution": { "width": 1280 }
                })),
                "later-patch".to_string(),
            )
            .await
            .expect("later patch should be persisted");
        assert_eq!(
            later,
            StorageSettingsMutation {
                revision: 2,
                applied: true
            }
        );

        let duplicate = storage
            .modify_user_settings(user.id, Some(initial_patch), "initial-patch".to_string())
            .await
            .expect("duplicate patch should be acknowledged");
        assert_eq!(
            duplicate,
            StorageSettingsMutation {
                revision: 2,
                applied: false
            }
        );

        let reloaded = load_test_storage(path.clone()).await;
        let user = reloaded
            .get_user(user.id)
            .await
            .expect("patched user should reload");
        assert_eq!(user.settings_revision, 2);
        assert_eq!(
            user.settings,
            Some(serde_json::json!({
                "bitrate": 22_000,
                "resolution": { "width": 1280 }
            }))
        );
        assert_eq!(
            user.settings_mutation_ids,
            vec!["initial-patch".to_string(), "later-patch".to_string()]
        );

        fs::remove_file(path)
            .await
            .expect("temporary storage file should be removed");
    }

    #[tokio::test]
    async fn recent_settings_mutation_history_is_bounded() {
        let storage = test_storage();
        let user = add_named_test_user(&storage, RoleId(1), "bounded-history-user").await;
        let mutation_count = MAX_RECENT_SETTINGS_MUTATIONS + 5;

        for index in 0..mutation_count {
            storage
                .modify_user_settings(
                    user.id,
                    Some(serde_json::json!({ "bitrate": index })),
                    format!("mutation-{index}"),
                )
                .await
                .expect("settings mutation should be accepted");
        }

        let user = storage
            .get_user(user.id)
            .await
            .expect("mutated user should exist");
        assert_eq!(user.settings_revision, mutation_count as u64);
        assert_eq!(
            user.settings_mutation_ids.len(),
            MAX_RECENT_SETTINGS_MUTATIONS
        );
        assert_eq!(user.settings_mutation_ids[0], "mutation-5");
        let expected_last = format!("mutation-{}", mutation_count - 1);
        assert_eq!(
            user.settings_mutation_ids.last().map(String::as_str),
            Some(expected_last.as_str())
        );
    }

    #[tokio::test]
    async fn invalid_settings_patch_does_not_advance_revision_or_history() {
        let storage = test_storage();
        let user = add_named_test_user(&storage, RoleId(1), "invalid-patch-user").await;

        assert!(matches!(
            storage
                .modify_user_settings(
                    user.id,
                    Some(serde_json::json!(["not", "an", "object"])),
                    "invalid-patch".to_string(),
                )
                .await,
            Err(AppError::BadRequest)
        ));

        let user = storage
            .get_user(user.id)
            .await
            .expect("user should still exist");
        assert!(user.settings.is_none());
        assert_eq!(user.settings_revision, 0);
        assert!(user.settings_mutation_ids.is_empty());
    }

    #[tokio::test]
    async fn durable_settings_write_surfaces_missing_parent_failure() {
        let path = temporary_storage_path("missing-parent")
            .with_extension("")
            .join("storage.json");
        let storage = load_test_storage(path).await;
        let user = add_named_test_user(&storage, RoleId(1), "failure-user").await;

        let result = storage
            .modify_user_settings(
                user.id,
                Some(serde_json::json!({ "bitrate": 10_000 })),
                "missing-parent".to_string(),
            )
            .await;

        assert!(matches!(result, Err(AppError::Io(_))));
    }

    #[tokio::test]
    async fn concurrent_durable_settings_writes_are_all_persisted() {
        let path = temporary_storage_path("concurrent-settings");
        let storage = load_test_storage(path.clone()).await;
        let first_user = add_named_test_user(&storage, RoleId(1), "concurrent-first").await;
        let second_user = add_named_test_user(&storage, RoleId(1), "concurrent-second").await;
        let first_settings = serde_json::json!({ "bitrate": 11_000 });
        let second_settings = serde_json::json!({ "bitrate": 22_000 });

        let (first_result, second_result) = tokio::join!(
            storage.modify_user_settings(
                first_user.id,
                Some(first_settings.clone()),
                "first-concurrent".to_string()
            ),
            storage.modify_user_settings(
                second_user.id,
                Some(second_settings.clone()),
                "second-concurrent".to_string()
            )
        );
        assert_eq!(
            first_result.expect("first durable write should succeed"),
            StorageSettingsMutation {
                revision: 1,
                applied: true
            }
        );
        assert_eq!(
            second_result.expect("second durable write should succeed"),
            StorageSettingsMutation {
                revision: 1,
                applied: true
            }
        );

        let reloaded = load_test_storage(path.clone()).await;
        assert_eq!(
            reloaded
                .get_user(first_user.id)
                .await
                .expect("first user should reload")
                .settings,
            Some(first_settings)
        );
        assert_eq!(
            reloaded
                .get_user(second_user.id)
                .await
                .expect("second user should reload")
                .settings,
            Some(second_settings)
        );

        fs::remove_file(path)
            .await
            .expect("temporary storage file should be removed");
    }

    #[tokio::test]
    async fn deleting_user_revokes_all_sessions() {
        let storage = test_storage();
        let user = add_test_user(&storage, RoleId(1)).await;
        let first_session = storage
            .create_session_token(user.id, Duration::from_secs(60))
            .await
            .expect("first session should be created");
        let second_session = storage
            .create_session_token(user.id, Duration::from_secs(60))
            .await
            .expect("second session should be created");

        storage
            .remove_user(user.id)
            .await
            .expect("user should be deleted");

        for session in [first_session, second_session] {
            assert!(matches!(
                storage.get_user_by_session_token(session).await,
                Err(AppError::SessionTokenNotFound)
            ));
        }
    }

    #[tokio::test]
    async fn stale_session_is_rejected_and_removed() {
        let storage = test_storage();
        let user = add_test_user(&storage, RoleId(1)).await;
        let session = storage
            .create_session_token(user.id, Duration::from_secs(60))
            .await
            .expect("session should be created");
        storage.users.write().await.remove(&user.id.0);

        assert!(matches!(
            storage.get_user_by_session_token(session).await,
            Err(AppError::SessionTokenNotFound)
        ));
        assert!(!storage.sessions.read().await.contains_key(&session));
    }

    #[tokio::test]
    async fn deleting_role_revokes_sessions_for_its_users() {
        let storage = test_storage();
        let role = storage
            .add_role(StorageRoleAdd {
                name: "test-role".to_string(),
                ty: RoleType::User,
                default_settings: StorageRoleDefaultSettings::default(),
                permissions: StorageRolePermissions::default(),
            })
            .await
            .expect("test role should be created");
        let user = add_test_user(&storage, role.id).await;
        let session = storage
            .create_session_token(user.id, Duration::from_secs(60))
            .await
            .expect("session should be created");

        storage
            .remove_role(role.id)
            .await
            .expect("role should be deleted");

        assert!(matches!(
            storage.get_user_by_session_token(session).await,
            Err(AppError::SessionTokenNotFound)
        ));
    }
}
