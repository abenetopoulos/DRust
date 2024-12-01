use actix_web::{post, web, HttpResponse, HttpServer, Responder, App};
use serde::{Serialize, Deserialize};

use crate::{
    conf::{NUM_SERVERS, WORKER_UNIT_SIZE, GLOBAL_HEAP_START}, drust_std::utils::{ResourceManager, COMPUTES},
    drust_std::{
        collections::dvec::{DVec, DVecRef},
        sync::dmutex::DMutex,
        thread::dspawn_to,
    },
};
use dmap::*;
use entry::GlobalEntry;
use tokio::task::JoinHandle;

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

pub async fn setup_frontend(map: DVecRef<'_, DMutex<GlobalEntry>>) {
    let map_ref = web::Data::new(map);
    println!("about to setup frontend on port 52017");

    HttpServer::new(move || {
        App::new().app_data(map_ref.clone()).service(web::resource("/schedule").to(process))
    }).bind(("0.0.0.0", 52017)).expect("failed to bind frontend to address").run().await;
}

pub async fn setup() {
    unsafe {
        COMPUTES = Some(ResourceManager::new(NUM_SERVERS));
    }

    let map = KVStore::new();
    let map_ref = web::Data::new(map.as_dref());

    {
        let mut handles = vec![];
        for i in 1..NUM_SERVERS {
            let map_ref = map.as_dref();
            let handle: JoinHandle<()> = dspawn_to(setup_frontend(map_ref), GLOBAL_HEAP_START + i * WORKER_UNIT_SIZE);
            handles.push(handle);
        }
        for handle in handles {
            handle.await;
        }
    }

    println!("about to setup frontend on port 52017");
    HttpServer::new(move || {
        App::new().app_data(map_ref.clone()).service(web::resource("/schedule").to(process))
    }).bind(("0.0.0.0", 52017)).expect("failed to bind frontend to address").run().await;
}

pub async fn process(
    map: web::Data<DVecRef<DMutex<GlobalEntry>>>,
    intent: web::Json<RequestPayload>,
) -> impl Responder {
    let map_ref = map.get_ref();
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
