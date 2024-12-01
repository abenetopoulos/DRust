use actix_web::{post, web, HttpResponse, HttpServer, Responder, App};
use serde::{Serialize, Deserialize};

use crate::{
    conf::NUM_SERVERS, drust_std::utils::{ResourceManager, COMPUTES},
    drust_std::{
        collections::dvec::{DVec, DVecRef},
        sync::dmutex::DMutex,
    },
};
use dmap::*;
use entry::GlobalEntry;

pub mod entry;
pub mod conf;
pub mod benchmark;
pub mod dmap;

#[derive(Serialize, Deserialize)]
pub struct RequestPayload {
    action: String,
    key: usize,
    value: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ResponsePayload {
    value: Option<String>,
}

// load column from file and return a Column struct
pub async fn run() {
    unsafe{
        COMPUTES = Some(ResourceManager::new(NUM_SERVERS));
    }
    benchmark::zipf_bench().await;
}

pub async fn setup() {
    unsafe {
        COMPUTES = Some(ResourceManager::new(NUM_SERVERS));
    }

    let map = web::Data::new(KVStore::new());

    println!("about to setup frontend on port 52017");
    HttpServer::new(move || {
        App::new().app_data(map.clone()).service(web::resource("/schedule").to(process))
    }).bind(("0.0.0.0", 52017)).expect("failed to bind frontend to address").run().await;
}

pub async fn process(
    map: web::Data<DVec<DMutex<GlobalEntry>>>,
    intent: web::Json<RequestPayload>,
) -> impl Responder {
    let map = map.get_ref();
    let map_ref = map.as_dref();
    let res = match intent.action.as_str() {
        "put" => {
            let key = intent.key;
            let mut value = ['x' as u8; 32];
            for (idx, byte) in intent.value.as_ref().unwrap().as_bytes().iter().enumerate() {
                value[idx] = *byte;
            }
            put(&map_ref, key, value).await;

            None
        },
        "get" => {
            let key = intent.key;
            get_safe(&map_ref, key).await
        }
        a @ _ => {
            eprintln!("unrecognized kvs command: {}", a);
            None
        },
    };

    match res {
        None => HttpResponse::Ok().json(ResponsePayload { value: None }),
        Some(ref r) => HttpResponse::Ok().json(ResponsePayload { value: Some(String::from_utf8_lossy(r).into_owned()) }),
    }
}
