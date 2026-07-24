use actix_web::{get, patch, web::Json};
use common::{api_bindings::StreamPermissions, api_bindings_ext::TsAny};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::app::{AppError, user::AuthenticatedUser};

fn convert_user_settings(settings: Value) -> Result<Option<Value>, AppError> {
    if settings.is_null() {
        Ok(None)
    } else if settings.is_object() {
        Ok(Some(settings))
    } else {
        Err(AppError::BadRequest)
    }
}

#[derive(Debug, Serialize, PartialEq)]
pub struct UserSettingsResponse {
    user_id: u32,
    settings: Option<Value>,
    revision: u64,
}

#[derive(Debug, Deserialize)]
pub struct PatchUserSettingsRequest {
    user_id: u32,
    settings: Value,
    mutation_id: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct PatchUserSettingsResponse {
    revision: u64,
    applied: bool,
}

fn verify_settings_owner(
    authenticated_user_id: u32,
    expected_user_id: u32,
) -> Result<(), AppError> {
    if authenticated_user_id == expected_user_id {
        Ok(())
    } else {
        Err(AppError::Forbidden)
    }
}

fn validate_mutation_id(mutation_id: &str) -> Result<(), AppError> {
    if mutation_id.trim().is_empty() || mutation_id.len() > 128 {
        Err(AppError::BadRequest)
    } else {
        Ok(())
    }
}

#[get("/settings/default")]
pub async fn get_default_settings(mut user: AuthenticatedUser) -> Result<Json<TsAny>, AppError> {
    let mut role = user.role().await?;

    let default_settings = role.default_settings().await?;

    Ok(Json(default_settings))
}

#[get("/settings/permissions")]
pub async fn get_permissions(
    mut user: AuthenticatedUser,
) -> Result<Json<StreamPermissions>, AppError> {
    let mut role = user.role().await?;

    let permissions = role.permissions().await?;

    Ok(Json(permissions))
}

#[get("/settings/user")]
pub async fn get_user_settings(
    mut user: AuthenticatedUser,
) -> Result<Json<UserSettingsResponse>, AppError> {
    let user_id = user.id().0;
    let (settings, revision) = user.settings().await?;

    Ok(Json(UserSettingsResponse {
        user_id,
        settings,
        revision,
    }))
}

#[patch("/settings/user")]
pub async fn patch_user_settings(
    mut user: AuthenticatedUser,
    Json(request): Json<PatchUserSettingsRequest>,
) -> Result<Json<PatchUserSettingsResponse>, AppError> {
    verify_settings_owner(user.id().0, request.user_id)?;
    validate_mutation_id(&request.mutation_id)?;

    let result = match convert_user_settings(request.settings)? {
        Some(settings) => user.patch_settings(settings, request.mutation_id).await?,
        None => user.clear_settings(request.mutation_id).await?,
    };

    Ok(Json(PatchUserSettingsResponse {
        revision: result.revision,
        applied: result.applied,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personal_settings_accept_objects_and_null_but_reject_other_json() {
        let settings = serde_json::json!({ "bitrate": 12_000 });
        assert_eq!(
            convert_user_settings(settings.clone()).expect("objects should be accepted"),
            Some(settings)
        );
        assert_eq!(
            convert_user_settings(Value::Null).expect("null should clear settings"),
            None
        );

        for invalid in [
            serde_json::json!([]),
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!("invalid"),
        ] {
            assert!(matches!(
                convert_user_settings(invalid),
                Err(AppError::BadRequest)
            ));
        }
    }

    #[test]
    fn settings_patch_is_bound_to_the_authenticated_user() {
        assert!(verify_settings_owner(17, 17).is_ok());
        assert!(matches!(
            verify_settings_owner(17, 18),
            Err(AppError::Forbidden)
        ));
    }

    #[test]
    fn mutation_ids_are_non_empty_and_bounded() {
        assert!(validate_mutation_id("mutation-1").is_ok());
        assert!(matches!(
            validate_mutation_id(""),
            Err(AppError::BadRequest)
        ));
        assert!(matches!(
            validate_mutation_id("   "),
            Err(AppError::BadRequest)
        ));
        assert!(validate_mutation_id(&"x".repeat(128)).is_ok());
        assert!(matches!(
            validate_mutation_id(&"x".repeat(129)),
            Err(AppError::BadRequest)
        ));
    }

    #[test]
    fn settings_responses_have_stable_wire_shapes() {
        let get = serde_json::to_value(UserSettingsResponse {
            user_id: 42,
            settings: Some(serde_json::json!({ "bitrate": 12_000 })),
            revision: 7,
        })
        .expect("GET settings response should serialize");
        assert_eq!(
            get,
            serde_json::json!({
                "user_id": 42,
                "settings": { "bitrate": 12_000 },
                "revision": 7
            })
        );

        let cleared = serde_json::to_value(UserSettingsResponse {
            user_id: 42,
            settings: None,
            revision: 8,
        })
        .expect("cleared GET settings response should serialize");
        assert_eq!(
            cleared,
            serde_json::json!({ "user_id": 42, "settings": null, "revision": 8 })
        );

        let patch = serde_json::to_value(PatchUserSettingsResponse {
            revision: 9,
            applied: false,
        })
        .expect("PATCH settings response should serialize");
        assert_eq!(
            patch,
            serde_json::json!({ "revision": 9, "applied": false })
        );

        let request: PatchUserSettingsRequest = serde_json::from_value(serde_json::json!({
            "user_id": 42,
            "settings": { "bitrate": 15_000 },
            "mutation_id": "mutation-42"
        }))
        .expect("PATCH settings request should deserialize");
        assert_eq!(request.user_id, 42);
        assert_eq!(request.settings, serde_json::json!({ "bitrate": 15_000 }));
        assert_eq!(request.mutation_id, "mutation-42");
    }
}
