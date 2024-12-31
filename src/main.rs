use axum::{
  extract::DefaultBodyLimit, http::{Method, StatusCode}, routing::{get, post, put}, Router
};
// use mongodb::action::StartTransaction;
use tower_http::cors::{CorsLayer, Any};


mod files;
mod options;
mod routes;

use routes::*;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
  let cors = CorsLayer::new()
  .allow_origin(Any)
  .allow_methods(vec![Method::GET, Method::POST, Method::PUT, Method::DELETE]);

  let app = Router::new()
    .route("/", get(welcome))
      .route("/upload", post(upload_asset))
      .route("/process", put(process_asset))
      .route("/check-file/:file_name", get(check_file))
      // The default axum body size limit is 2MiB, so we increase it to 1GiB.
      .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
      .layer(cors)
      .into_make_service();
  let ip = dotenv::var("LOCAL_ADDRESS").unwrap_or(String::from("0.0.0.0"));
  let port = dotenv::var("PORT").unwrap_or(String::from("3000"));
  let address = format!("{}:{}", ip, port);
  let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
  axum::serve(listener, app).await.unwrap();
  Ok(())
}
