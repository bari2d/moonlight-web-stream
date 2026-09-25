use actix_web::{HttpResponse, post, web::Json};
use tracing::info;
use serde::Deserialize;

use crate::app::user::AuthenticatedUser;

const MAX_LINES: usize = 200;
const MAX_LINE_LEN: usize = 2000;

#[derive(Deserialize)]
struct PostClientLogRequest {
    session: String,
    user_agent: String,
    lines: Vec<String>,
}

/// Mirrors the browser's stream debug log into the server log, so clients
/// without devtools (e.g. iOS Safari) can still be diagnosed.
#[post("/client-log")]
async fn post_client_log(
    _user: AuthenticatedUser,
    Json(request): Json<PostClientLogRequest>,
) -> HttpResponse {
    let session: String = request.session.chars().take(16).collect();
    let user_agent: String = request.user_agent.chars().take(200).collect();

    for line in request.lines.iter().take(MAX_LINES) {
        let line: String = line.chars().take(MAX_LINE_LEN).collect();
        info!(target: "client_log", "[{session}] {line} ua={user_agent}");
    }

    HttpResponse::NoContent().finish()
}
