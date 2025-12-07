use axum::body::Body;
use axum::response::IntoResponse;
use axum::routing::options;
use axum::{Extension, Router};
use surrealdb_core::dbs::Session;
use bytes::Bytes;
use http::StatusCode;

use super::AppState;
use crate::cnf::HTTP_MAX_SQL_BODY_SIZE;
use axum::extract::{DefaultBodyLimit, Path};
use tower_http::limit::RequestBodyLimitLayer;
use super::error::ResponseError;
use axum::response::Response;
use anyhow::Error;

pub(super) fn router<S>() -> Router<S>
where
	S: Clone + Send + Sync + 'static,
{
	Router::new()
		.route("/file/{ns}/{db}/{bucket}/{*path}", options(|| async {}).get(get_handler))
		.route_layer(DefaultBodyLimit::disable())
		.layer(RequestBodyLimitLayer::new(*HTTP_MAX_SQL_BODY_SIZE))
}

async fn get_handler(
	Extension(state): Extension<AppState>,
	Extension(session): Extension<Session>,
	Path((ns, db, bucket, path)): Path<(String, String, String, String)>,
) -> Result<impl IntoResponse, ResponseError> {

	let ds = &state.datastore;

	let mut data = ds.get_file(&ns, &db, &bucket, &path)
		.await
		.map_err(ResponseError)?;

	// Create a bounded channel for streaming the file content
	let (sender, receiver) = surrealdb::channel::bounded::<Result<Bytes, anyhow::Error>>(1);
	let body = Body::from_stream(receiver);

	// Spawn a task to send the file data through the channel
	tokio::spawn(async move {
		match Bytes::try_from(data) {
			Ok(bytes) => {
				let _ = sender.send(Ok(bytes)).await;
			}
			Err(e) => {
				let _ = sender.send(Err(anyhow::anyhow!("Failed to convert file data to Bytes: {}", e))).await;
			}
		}
	});
	// Return the streamed body
	Ok((Response::builder()
		.status(StatusCode::OK)
		.header("Content-Type", "application/octet-stream")
		.body(body)
		.map_err(|e| ResponseError(Error::from(e))))?)
}
