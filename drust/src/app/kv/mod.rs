use actix_web::{post, web, HttpResponse, HttpServer, Responder, App};
use serde::{Serialize, Deserialize};

use crate::{conf::NUM_SERVERS, drust_std::utils::{ResourceManager, COMPUTES}};
use crate::drust_std::collections::dvec::{DVec, DVecRef};
use dmap::KVStore;

pub mod entry;
pub mod conf;
pub mod benchmark;
pub mod dmap;

#[derive(Serialize, Deserialize)]
pub struct RequestPayload {
    action: String,
    key: usize,
    value: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct ResponsePayload {
    value: Option<usize>,
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
    }).bind(("0.0.0.0", 52017)).run().await;
}

pub async fn process(
    map: Data<DVec<DMutex<GlobalEntry>>>,
    intent: web::Json<NandoActivationIntentSerializable>,
) -> impl Responder {
    let res = match &intent.action {
        "put" => {
            let key = intent.key;
            let value = intent.value.unwrap();
            put(&map, key, value).await
        },
        "get" => {
            let key = intent.key;
            get(&map, key).await
        }
        a @ _ => {
            eprintln!("unrecognized kvs command: {}", a);
            None
        },
    };

    HttpResponse::Ok().json(ResponsePayload { value: res })
}
