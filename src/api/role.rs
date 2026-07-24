use actix_web::{
    HttpResponse, delete, get, patch, post,
    web::{Data, Json, Query},
};
use common::{
    api_bindings::{
        DeleteRoleQuery, GetRoleQuery, GetRoleResponse, GetRolesResponse, PatchRoleRequest,
        PostRoleRequest, PostRoleResponse, StreamPermissions,
    },
    api_bindings_ext::TsAny,
};

use futures::future::join_all;
use tracing::warn;

use crate::app::{
    App, AppError,
    role::RoleId,
    storage::{
        StorageRoleAdd, StorageRoleDefaultSettings, StorageRoleModify, StorageRolePermissions,
    },
    user::{Admin, AuthenticatedUser},
};

fn convert_settings(settings: TsAny) -> StorageRoleDefaultSettings {
    StorageRoleDefaultSettings {
        value: settings.into(),
    }
}
fn convert_permissions(permissions: StreamPermissions) -> StorageRolePermissions {
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

fn convert_role_modify(request: PatchRoleRequest) -> (RoleId, StorageRoleModify) {
    let defaults_only = request.name.is_none()
        && request.permissions.is_none()
        && request.default_settings.is_some();

    (
        RoleId(request.id),
        StorageRoleModify {
            name: request.name,
            ty: (!defaults_only).then(|| request.ty.into()),
            permissions: request.permissions.map(convert_permissions),
            default_settings: request.default_settings.map(convert_settings),
        },
    )
}

#[post("/role")]
pub async fn add_role(
    app: Data<App>,
    admin: Admin,
    Json(request): Json<PostRoleRequest>,
) -> Result<Json<PostRoleResponse>, AppError> {
    let PostRoleRequest {
        name,
        ty,
        default_settings,
        permissions,
    } = request;

    let mut role = app
        .add_role(
            &admin,
            StorageRoleAdd {
                name,
                ty: ty.into(),
                default_settings: convert_settings(default_settings),
                permissions: convert_permissions(permissions),
            },
        )
        .await?;

    Ok(Json(PostRoleResponse {
        role: role.detailed_role().await?,
    }))
}

#[get("/role")]
pub async fn get_role(
    app: Data<App>,
    mut user: AuthenticatedUser,
    Query(query): Query<GetRoleQuery>,
) -> Result<Json<GetRoleResponse>, AppError> {
    let role_id = match query.id {
        Some(id) => RoleId(id),
        None => user.role_id().await?,
    };

    let mut role = app.role_by_id(role_id).await?;

    Ok(Json(GetRoleResponse {
        role: role.detailed_role().await?,
    }))
}

#[patch("/role")]
pub async fn patch_role(
    app: Data<App>,
    admin: Admin,
    Json(request): Json<PatchRoleRequest>,
) -> Result<HttpResponse, AppError> {
    let (role_id, modify) = convert_role_modify(request);

    let role = app.role_by_id(role_id).await?;

    role.modify(&admin, modify).await?;

    Ok(HttpResponse::Ok().finish())
}

#[delete("/role")]
pub async fn delete_role(
    app: Data<App>,
    admin: Admin,
    Query(query): Query<DeleteRoleQuery>,
) -> Result<HttpResponse, AppError> {
    let role_id = RoleId(query.id);

    let role = app.role_by_id(role_id).await?;

    role.delete(&admin).await?;

    Ok(HttpResponse::Ok().finish())
}

#[get("/roles")]
pub async fn list_roles(app: Data<App>, admin: Admin) -> Result<Json<GetRolesResponse>, AppError> {
    let mut roles = app.all_roles(&admin).await?;

    let role_results = join_all(roles.iter_mut().map(|role| role.undetailed_role())).await;

    let mut out_roles = Vec::with_capacity(role_results.len());
    for (result, role) in role_results.into_iter().zip(roles) {
        match result {
            Ok(role) => {
                out_roles.push(role);
            }
            Err(err) => {
                warn!("Failed to query detailed role of {role:?}: {err}");
            }
        }
    }

    Ok(Json(GetRolesResponse { roles: out_roles }))
}

#[cfg(test)]
mod tests {
    use common::api_bindings::RoleType as ApiRoleType;

    use super::*;
    use crate::app::user::RoleType;

    #[test]
    fn full_role_patch_preserves_submitted_role_type() {
        let (_, modify) = convert_role_modify(PatchRoleRequest {
            id: 42,
            name: Some("Administrators".to_string()),
            ty: ApiRoleType::Admin,
            default_settings: None,
            permissions: None,
        });

        assert_eq!(modify.ty, Some(RoleType::Admin));
    }

    #[test]
    fn defaults_only_patch_does_not_rewrite_role_type() {
        let (_, modify) = convert_role_modify(PatchRoleRequest {
            id: 42,
            name: None,
            ty: ApiRoleType::Admin,
            default_settings: Some(serde_json::json!({ "bitrate": 12_000 }).into()),
            permissions: None,
        });

        assert_eq!(modify.ty, None);
        assert!(modify.default_settings.is_some());
    }
}
