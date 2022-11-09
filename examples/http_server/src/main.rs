use std::collections::HashMap;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use raindb::{Batch, DbOptions, RainDBError, ReadOptions, WriteOptions, DB};
use rocket::http::Status;
use rocket::response::Responder;
use rocket::serde::json::Json;
use rocket::State;

#[macro_use]
extern crate rocket;

#[get("/")]
fn index() -> &'static str {
    "It's raining"
}

#[get("/?<key>")]
fn get(key: String, raindb: &State<Arc<DB>>) -> Result<String, WrappedRainDBError> {
    match raindb.get(ReadOptions::default(), key.as_bytes()) {
        Err(read_err) => Err(read_err.into()),
        Ok(read_result) => {
            let decoded_result = str::from_utf8(&read_result).unwrap();
            Ok(decoded_result.to_owned())
        }
    }
}

#[put("/?<key>&<value>")]
fn put(key: String, value: String, raindb: &State<Arc<DB>>) {
    raindb
        .put(WriteOptions::default(), key.into(), value.into())
        .unwrap();
}

#[put("/batch", format = "application/json", data = "<batch>")]
fn batch_commit(batch: Json<HashMap<&str, &str>>, raindb: &State<Arc<DB>>) {
    let mut db_batch: Batch = Batch::new();
    for (key, value) in batch.iter() {
        db_batch.add_put((*key).into(), (*value).into());
    }

    raindb.apply(WriteOptions::default(), db_batch).unwrap();
}

#[delete("/?<key>")]
fn delete(key: String, raindb: &State<Arc<DB>>) {
    raindb.delete(WriteOptions::default(), key.into()).unwrap();
}

#[launch]
fn rocket() -> _ {
    let db_path = PathBuf::from("http_raindb");
    log::info!("Opening RainDB instance at {db_path:?}");
    let db = DB::open(DbOptions {
        create_if_missing: true,
        db_path: db_path.to_str().unwrap().to_owned(),
        ..DbOptions::default()
    })
    .unwrap();
    let wrapped_db = Arc::new(db);

    rocket::build()
        .mount("/", routes![index, get, put, batch_commit, delete])
        .manage(wrapped_db)
}

struct WrappedRainDBError(RainDBError);

impl<'r> Responder<'r, 'static> for WrappedRainDBError {
    fn respond_to(self, request: &'r rocket::Request<'_>) -> rocket::response::Result<'static> {
        match self.0 {
            RainDBError::KeyNotFound => Status::NotFound.respond_to(request),
            _ => Status::InternalServerError.respond_to(request),
        }
    }
}

impl From<RainDBError> for WrappedRainDBError {
    fn from(err: RainDBError) -> Self {
        WrappedRainDBError(err)
    }
}
