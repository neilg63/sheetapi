use bson::{doc, oid::ObjectId, Bson, Document};
use futures::stream::StreamExt;
use mongodb::options::Compressor;
use mongodb::{
    options::{AggregateOptions, ClientOptions, FindOptions},
    Client, Collection,
};
use serde_json::Value;
use serde_with::chrono;
use std::str::FromStr;
use std::vec;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use crate::options::DataSetMatcher;

const DEFAULT_MONGO_URI: &str = "mongodb://localhost:27017";
const DEFAULT_MONGO_CONNECTION_TIMEOUT: u64 = 6000;
const DEFAULT_MONGO_MIN_POOL_SIZE: u32 = 2;
const DEFAULT_MONGO_MAX_POOL_SIZE: u32 = 64;

pub struct DatabaseConfig {
    pub uri: String,
    pub connection_timeout: Option<Duration>,
    pub min_pool_size: Option<u32>,
    pub max_pool_size: Option<u32>,
    pub compressors: Option<Vec<Compressor>>,
}

impl DatabaseConfig {
    pub fn new() -> Self {
        let mongo_uri: String =
            dotenv::var("MONGO_URI").unwrap_or_else(|_| DEFAULT_MONGO_URI.to_string());

        let mongo_connection_timeout: u64 = match dotenv::var("MONGO_CONNECTION_TIMEOUT") {
            Ok(value) => value.parse().unwrap_or(DEFAULT_MONGO_CONNECTION_TIMEOUT),
            Err(_) => DEFAULT_MONGO_CONNECTION_TIMEOUT,
        };

        let mongo_min_pool_size: u32 = match dotenv::var("MONGO_MIN_POOL_SIZE") {
            Ok(value) => value.parse().unwrap_or(DEFAULT_MONGO_MIN_POOL_SIZE),
            Err(_) => DEFAULT_MONGO_MIN_POOL_SIZE,
        };

        let mongo_max_pool_size: u32 = match dotenv::var("MONGO_MAX_POOL_SIZE") {
            Ok(value) => value.parse().unwrap_or(DEFAULT_MONGO_MAX_POOL_SIZE),
            Err(_) => DEFAULT_MONGO_MAX_POOL_SIZE,
        };

        Self {
            uri: mongo_uri,
            connection_timeout: Some(Duration::from_secs(mongo_connection_timeout)),
            min_pool_size: Some(mongo_min_pool_size),
            max_pool_size: Some(mongo_max_pool_size),
            compressors: Some(vec![
                Compressor::Snappy,
                Compressor::Zlib {
                    level: Default::default(),
                },
                Compressor::Zstd {
                    level: Default::default(),
                },
            ]),
        }
    }
}

#[derive(Clone)]
pub struct DB {
    pub client: Arc<Mutex<Client>>,
}

impl DB {
    pub async fn new() -> Self {
        let database_config = DatabaseConfig::new();
        let mut client_options = ClientOptions::parse(&database_config.uri).await.unwrap();
        client_options.connect_timeout = database_config.connection_timeout;
        client_options.max_pool_size = database_config.max_pool_size;
        client_options.min_pool_size = database_config.min_pool_size;
        client_options.compressors = database_config.compressors;
        let client = Client::with_options(client_options).unwrap();
        DB {
            client: Arc::new(Mutex::new(client)),
        }
    }
    

    pub async fn get_collection(&self, collection_name: &str) -> Collection<Document> {
        let db_name = get_db_name();
        let db_client = self.client.lock().await;
        db_client
            .database(&db_name)
            .collection::<Document>(collection_name)
    }

    pub async fn find_records(
        &self,
        collection_name: &str,
        limit: u64,
        skip: u64,
        filter_options: Option<Document>,
        fields: Option<Vec<&str>>,
    ) -> Vec<Document> {
        let collection = self.get_collection(collection_name).await;
        let max = if limit > 0 { limit as i64 } else { 10000000i64 };
        let mut projection: Option<Document> = None;
        if let Some(field_list) = fields {
            let mut doc = doc! {};
            for field in field_list {
                doc.insert(field, 1);
            }
            projection = Some(doc);
        }
        let find_options = FindOptions::builder()
            .projection(projection)
            .skip(skip)
            .limit(max)
            .build();
        let filter_opts = if let Some(fo) = filter_options {
            fo
        } else {
            doc! {}
        };
        let cursor_r = collection
            .find(filter_opts)
            .with_options(find_options)
            .await;
        if let Ok(cursor) = cursor_r {
            let results: Vec<mongodb::error::Result<Document>> = cursor.collect::<Vec<_>>().await;
            let mut rows: Vec<Document> = Vec::new();
            if results.len() > 0 {
                for item in results {
                    if let Ok(row) = item {
                        rows.push(row);
                    }
                }
            }
            return rows;
        }
        vec![]
    }


    pub async fn fetch_record(
        &self,
        collection_name: &str,
        filter_options: Option<Document>,
    ) -> Option<Document> {
        let records = self
            .find_records(collection_name, 1, 0, filter_options, None)
            .await;
        if records.len() > 0 {
            for row in records {
                return Some(row);
            }
        }
        None
    }


    pub async fn fetch_dataset(&self, dataset_id: &str, import_id_opt: Option<String>, filter_options: Option<Document>, limit: u64, skip: u64) -> Option<RowSet> {
        let collection: Collection<Document> = self.get_collection("datasets").await;
        if let Ok(id) = ObjectId::from_str(&dataset_id) {

            let cursor_r = collection.find_one(doc!{ "_id": id }).await;
            if let Ok(doc_opt) = cursor_r {
                if let Some(dset) = doc_opt {
                    let mut criteria = doc! { "dataset_id": id };
                    if let Some(filter) = filter_options {
                        for (k, v) in filter.iter() {
                            if let Some(d) = v.as_document() {
                                criteria.insert(k, d );  
                            }
                        }
                    }
                    let row_docs = self.find_records("data_rows", limit, skip, Some(criteria), None).await;
                    let rows = row_docs.iter().filter(|r| r.contains_key("data")).map(|r| r.get("data").unwrap().as_document().unwrap().to_owned()).collect::<Vec<Document>>();
                    return Some(RowSet::new(&dset, &rows, rows.len() as u64, limit, skip));
                }
            }
        }
        None
    }

    pub async fn update_record(
        &self,
        collection_name: &str,
        filter_options: &Document,
        values: &Document,
        add: Option<(&str, &Document, Option<ObjectId>)>,
    ) -> (bool, bool, Option<Bson>) {
        let collection: Collection<Document> = self.get_collection(collection_name).await;

        let cursor_r1 = collection.find_one(filter_options.to_owned()).await;
        let record: Option<Document> = match cursor_r1 {
            Ok(doc_opt) => doc_opt,
            Err(_) => None,
        };
        if let Some(doc) = record {
            let mut set_data = doc! { "options": values.to_owned() };
            let mut add_to_set = false;

            if let Some((key, doc, oid_opt)) = add {
                if let Some(oid) = oid_opt {
                    if let Some(inner_element) = doc.get(key) {
                        if let Some(inner_vec) = inner_element.as_array() {
                            let mut inner_items = inner_vec
                                .iter()
                                .map(|b| b.as_document().unwrap())
                                .collect::<Vec<&Document>>();
                            if let Some(i_index) = inner_vec
                                .iter()
                                .position(|x| x.as_object_id().unwrap() == oid)
                            {
                                inner_items[i_index] = doc;
                                set_data.insert(key, inner_items);
                            }
                        }
                    }
                } else {
                    add_to_set = true;
                }
            }
            let mut update = doc! { "$set": set_data };
            if add_to_set {
                let import_row = doc! {
                    "_id": ObjectId::new(),
                    "dt": chrono::Utc::now(),
                    "filename": "test",
                    "sheet_index": 0
                };
                update.insert("$addToSet", doc! { "imports": import_row });
            }
            let cursor_r2 = collection
                .update_one(filter_options.to_owned(), update)
                .await;
            if let Ok(cursor) = cursor_r2 {
                if let Some(id) = cursor.upserted_id {
                    return (true, true, Some(id));
                }
                return (true, false, None);
            }
        }
        (false, false, None)
    }

    pub async fn insert_record(
        &self,
        collection_name: &str,
        values: &Document,
    ) -> Option<Document> {
        let collection: Collection<Document> = self.get_collection(collection_name).await;
        let cursor_r = collection.insert_one(values.to_owned()).await;
        if let Ok(cursor) = cursor_r {
            if let Some(id) = cursor.inserted_id.as_object_id() {
                let filter = doc! { "_id": id };
                let row = collection.find_one(filter).await;
                if let Ok(doc_opt) = row {
                    return doc_opt;
                }
            }
        }
        None
    }

    pub async fn insert_many(
        &self,
        collection_name: &str,
        rows: &[Document],
    ) -> Option<HashMap<usize, Bson>> {
        let collection: Collection<Document> = self.get_collection(collection_name).await;
        let cursor_r = collection.insert_many(rows).await;
        if let Ok(cursor) = cursor_r {
            return Some(cursor.inserted_ids);
        }
        None
    }

    pub async fn fetch_aggregated_with_options(
        &self,
        collection_name: &str,
        pipeline: Vec<Document>,
        options: Option<AggregateOptions>,
    ) -> Vec<Document> {
        let mut rows: Vec<Document> = vec![];
        let collection: Collection<Document> =  self.get_collection(collection_name).await;
        let cursor = if let Some(agg_options) = options {
            collection
                .aggregate(pipeline)
                .with_options(agg_options)
                .await
                .expect("could not load data.")
        } else {
            collection
                .aggregate(pipeline)
                .await
                .expect("could not load data.")
        };
        let results: Vec<mongodb::error::Result<Document>> = cursor.collect().await;
        if results.len() > 0 {
            for item in results {
                if let Ok(row) = item {
                    rows.push(row);
                }
            }
        }
        rows
    }

    pub async fn fetch_aggregated(
        &self,
        collection_name: &str,
        pipeline: Vec<Document>,
    ) -> Vec<Document> {
        self.fetch_aggregated_with_options(collection_name, pipeline, None)
            .await
    }

    pub async fn find_by_name_and_index(&self, name: &str, index: u32) -> Option<Document> {
        let filter = doc! { "filename": name, "sheet_index": index };
        self.fetch_record("imports", Some(filter)).await
    }

    pub async fn update_import(
        &self,
        filter: &Document,
        values: &mut Document,
        import_id_opt: Option<ObjectId>,
    ) -> Option<(ObjectId, ObjectId)> {
        values.insert("updated_at", chrono::Utc::now());
        let name = values.get_str("filename").unwrap_or_default();
        let sheet_index = values.get_i32("sheet_index").unwrap_or(0);
        let import = doc! {
            "_id": ObjectId::new(),
            "dt": chrono::Utc::now(),
            "filename": name,
            "sheet_index": sheet_index
        };
        let (_updated, _exists, id_opt) = self
            .update_record(
                "datasets",
                &filter,
                values,
                Some(("imports", &import, import_id_opt)),
            )
            .await;
        if let Some(id) = id_opt {
            let oid = id.as_object_id().unwrap();
            return Some((oid, id.as_object_id().unwrap()));
        }
        None
    }

    pub async fn save_rows(
        &self,
        dataset_id: ObjectId,
        import_id: ObjectId,
        rows: &[Value],
    ) -> usize {
        let docs = rows
            .iter()
            .map(|row| {
                doc! { "dataset_id": dataset_id, "import_id": import_id, "data": bson::to_document(row).unwrap() }
            })
            .collect::<Vec<Document>>();
        if let Some(id) = self.insert_many("data_rows", &docs).await {
            return id.len();
        }
        0
    }

    pub async fn save_import(
        &self,
        options: &Value,
        import_id: Option<String>,
    ) -> Option<(ObjectId, ObjectId)> {
        let mut doc = bson::to_document(options).unwrap();
        let dataset_id_opt = options["dataset_id"].as_str();
        let fname = options["filename"].as_str().unwrap_or_default().to_owned();
        let s_index = options["sheet_index"].as_u64().unwrap_or(0) as u32;
        let matcher = if let Some(dataset_id) = dataset_id_opt {
            DataSetMatcher::from_id(dataset_id)
        } else {
            DataSetMatcher::from_name_index(&fname, s_index)
        };
        let import_id_opt = import_id.map(|id| ObjectId::from_str(&id).unwrap());
        if let Some((id, import_id)) = self
            .update_import(&matcher.to_criteria(), &mut doc, import_id_opt)
            .await
        {
            return Some((id, import_id));
        }
        let import = doc! {
            "_id": ObjectId::new(),
            "dt": chrono::Utc::now(),
            "filename": &fname,
            "sheet_index": s_index
        };
        let save_dac = doc! {
            "name": &fname,
            "sheet_index": s_index,
            "options": doc.clone(),
            "imports": [import],
            "created_at": chrono::Utc::now()
        };
        if let Some(record) = self.insert_record("datasets", &save_dac).await {
            let _id = if let Some(id_opt) = record.get("_id") {
                id_opt.as_object_id()
            } else {
                return None;
            };
            if let Some(import_element) = record.get("imports") {
                if let Some(imports) = import_element.as_array() {
                    if let Some(last_import) = imports.last() {
                        println!("{:?}", last_import);
                        if let Some(imp_id) = last_import.as_document().and_then(|doc| doc.get("_id")) {
                            return Some((_id.unwrap(), imp_id.as_object_id().unwrap()));
                        }
                    }
                }
            }
        }
        None
    }

    pub async fn save_import_with_rows(
        &self,
        options: &Value,
        rows: &[Value],
        import_id: Option<String>,
    ) -> Option<(String, String, usize)> {
        if let Some((id, import_id)) = self.save_import(options, import_id).await {
            let id_string = id.to_string();
            let import_id_string = import_id.to_string();
            let count = self.save_rows(id, import_id, rows).await;
            return Some((id_string, import_id_string, count));
        }
        None
    }
}

fn get_db_name() -> String {
    std::env::var("MONGO_NAME").expect("Failed to load `MONGO_DB_NAME` environment variable.")
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RowSet {
    pub dataset: Document,
    pub rows: Vec<Document>,
    pub total: u64,
    pub limit: u64,
    pub skip: u64,
}

impl RowSet {
    pub fn new(dataset: &Document, rows: &[Document], total: u64, limit: u64, skip: u64) -> Self {
        Self {
            dataset: dataset.to_owned(),
            rows: rows.to_owned(),
            total,
            limit,
            skip,
        }
    }
}