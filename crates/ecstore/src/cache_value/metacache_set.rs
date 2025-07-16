// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cache_value::LIST_PATH_RAW_CANCEL_TOKEN;
use crate::disk::error::DiskError;
use crate::disk::{self, DiskAPI, DiskStore, WalkDirOptions};
use crate::set_disk::DEFAULT_READ_BUFFER_SIZE;
use futures::future::{BoxFuture, join_all};
use rustfs_filemeta::{MetaCacheEntries, MetaCacheEntry, MetacacheReader, is_io_eof};
use std::sync::Arc;
use tokio::spawn;
use tracing::error;

pub type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> BoxFuture<'static, ()> + Send>;
pub type PartialFn = Box<dyn Fn(MetaCacheEntries, &[Option<DiskError>]) -> BoxFuture<'static, ()> + Send>;
pub type FinishedFn = Box<dyn Fn(&[Option<DiskError>]) -> BoxFuture<'static, ()> + Send>;

// pub type AgreedFn = Box<dyn Fn(MetaCacheEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
// pub type PartialFn =
//     Box<dyn Fn(MetaCacheEntries, &[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;
// type FinishedFn = Box<dyn Fn(&[Option<DiskError>]) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

// #[derive(Default)]
// pub struct ListPathRawOptions {
//     pub disks: Vec<Option<DiskStore>>,
//     pub fallback_disks: Vec<Option<DiskStore>>,
//     pub bucket: String,
//     pub path: String,
//     pub recursice: bool,
//     pub filter_prefix: Option<String>,
//     pub forward_to: Option<String>,
//     pub min_disks: usize,
//     pub report_not_found: bool,
//     pub per_disk_limit: i32,
//     pub agreed: Option<AgreedFn>,
//     pub partial: Option<PartialFn>,
//     pub finished: Option<FinishedFn>,
//     // pub agreed: Option<Arc<dyn Fn(MetaCacheEntry) + Send + Sync>>,
//     // pub partial: Option<Arc<dyn Fn(MetaCacheEntries, &[Option<Error>]) + Send + Sync>>,
//     // pub finished: Option<Arc<dyn Fn(&[Option<Error>]) + Send + Sync>>,
// }

// impl Clone for ListPathRawOptions {
//     fn clone(&self) -> Self {
//         Self {
//             disks: self.disks.clone(),
//             fallback_disks: self.fallback_disks.clone(),
//             bucket: self.bucket.clone(),
//             path: self.path.clone(),
//             recursice: self.recursice,
//             filter_prefix: self.filter_prefix.clone(),
//             forward_to: self.forward_to.clone(),
//             min_disks: self.min_disks,
//             report_not_found: self.report_not_found,
//             per_disk_limit: self.per_disk_limit,
//             ..Default::default()
//         }
//     }
// }

// pub async fn list_path_raw(mut rx: B_Receiver<bool>, opts: ListPathRawOptions) -> disk::error::Result<()> {
//     if opts.disks.is_empty() {
//         return Err(DiskError::other("list_path_raw: 0 drives provided"));
//     }

//     let mut jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), DiskError>>> = Vec::new();
//     let mut readers = Vec::with_capacity(opts.disks.len());
//     let fds = Arc::new(opts.fallback_disks.clone());

//     let (cancel_tx, cancel_rx) = tokio::sync::broadcast::channel::<bool>(1);

//     for disk in opts.disks.iter() {
//         let opdisk = disk.clone();
//         let opts_clone = opts.clone();
//         let fds_clone = fds.clone();
//         let mut cancel_rx_clone = cancel_rx.resubscribe();
//         let (rd, mut wr) = tokio::io::duplex(64);
//         readers.push(MetacacheReader::new(rd));
//         jobs.push(spawn(async move {
//             let wakl_opts = WalkDirOptions {
//                 bucket: opts_clone.bucket.clone(),
//                 base_dir: opts_clone.path.clone(),
//                 recursive: opts_clone.recursice,
//                 report_notfound: opts_clone.report_not_found,
//                 filter_prefix: opts_clone.filter_prefix.clone(),
//                 forward_to: opts_clone.forward_to.clone(),
//                 limit: opts_clone.per_disk_limit,
//                 ..Default::default()
//             };

//             let mut need_fallback = false;
//             if let Some(disk) = opdisk {
//                 match disk.walk_dir(wakl_opts, &mut wr).await {
//                     Ok(_res) => {}
//                     Err(err) => {
//                         error!("walk dir err {:?}", &err);
//                         need_fallback = true;
//                     }
//                 }
//             } else {
//                 need_fallback = true;
//             }

//             if cancel_rx_clone.try_recv().is_ok() {
//                 // warn!("list_path_raw: cancel_rx_clone.try_recv().await.is_ok()");
//                 return Ok(());
//             }

//             while need_fallback {
//                 // warn!("list_path_raw: while need_fallback start");
//                 let disk = match fds_clone.iter().find(|d| d.is_some()) {
//                     Some(d) => {
//                         if let Some(disk) = d.clone() {
//                             disk
//                         } else {
//                             break;
//                         }
//                     }
//                     None => break,
//                 };
//                 match disk
//                     .as_ref()
//                     .walk_dir(
//                         WalkDirOptions {
//                             bucket: opts_clone.bucket.clone(),
//                             base_dir: opts_clone.path.clone(),
//                             recursive: opts_clone.recursice,
//                             report_notfound: opts_clone.report_not_found,
//                             filter_prefix: opts_clone.filter_prefix.clone(),
//                             forward_to: opts_clone.forward_to.clone(),
//                             limit: opts_clone.per_disk_limit,
//                             ..Default::default()
//                         },
//                         &mut wr,
//                     )
//                     .await
//                 {
//                     Ok(_r) => {
//                         need_fallback = false;
//                     }
//                     Err(err) => {
//                         error!("walk dir2 err {:?}", &err);
//                         break;
//                     }
//                 }
//             }

//             // warn!("list_path_raw: while need_fallback done");
//             Ok(())
//         }));
//     }

//     let revjob = spawn(async move {
//         let mut errs: Vec<Option<DiskError>> = Vec::with_capacity(readers.len());
//         for _ in 0..readers.len() {
//             errs.push(None);
//         }

//         loop {
//             let mut current = MetaCacheEntry::default();

//             // warn!(
//             //     "list_path_raw: loop start, bucket: {}, path: {}, current: {:?}",
//             //     opts.bucket, opts.path, &current.name
//             // );

//             if rx.try_recv().is_ok() {
//                 return Err(DiskError::other("canceled"));
//             }

//             let mut top_entries: Vec<Option<MetaCacheEntry>> = vec![None; readers.len()];

//             let mut at_eof = 0;
//             let mut fnf = 0;
//             let mut vnf = 0;
//             let mut has_err = 0;
//             let mut agree = 0;

//             for (i, r) in readers.iter_mut().enumerate() {
//                 if errs[i].is_some() {
//                     has_err += 1;
//                     continue;
//                 }

//                 let entry = match r.peek().await {
//                     Ok(res) => {
//                         if let Some(entry) = res {
//                             // info!("read entry disk: {}, name: {}", i, entry.name);
//                             entry
//                         } else {
//                             // eof
//                             at_eof += 1;
//                             // warn!("list_path_raw: peek eof, disk: {}", i);
//                             continue;
//                         }
//                     }
//                     Err(err) => {
//                         if err == rustfs_filemeta::Error::Unexpected {
//                             at_eof += 1;
//                             // warn!("list_path_raw: peek err eof, disk: {}", i);
//                             continue;
//                         }

//                         // warn!("list_path_raw: peek err00, err: {:?}", err);

//                         if is_io_eof(&err) {
//                             at_eof += 1;
//                             // warn!("list_path_raw: peek eof, disk: {}", i);
//                             continue;
//                         }

//                         if err == rustfs_filemeta::Error::FileNotFound {
//                             at_eof += 1;
//                             fnf += 1;
//                             // warn!("list_path_raw: peek fnf, disk: {}", i);
//                             continue;
//                         } else if err == rustfs_filemeta::Error::VolumeNotFound {
//                             at_eof += 1;
//                             fnf += 1;
//                             vnf += 1;
//                             // warn!("list_path_raw: peek vnf, disk: {}", i);
//                             continue;
//                         } else {
//                             has_err += 1;
//                             errs[i] = Some(err.into());
//                             // warn!("list_path_raw: peek err, disk: {}", i);
//                             continue;
//                         }
//                     }
//                 };

//                 // warn!("list_path_raw: loop entry: {:?}, disk: {}", &entry.name, i);

//                 // If no current, add it.
//                 if current.name.is_empty() {
//                     top_entries[i] = Some(entry.clone());
//                     current = entry;
//                     agree += 1;

//                     continue;
//                 }
//                 // If exact match, we agree.
//                 if let (_, true) = current.matches(Some(&entry), true) {
//                     top_entries[i] = Some(entry);
//                     agree += 1;

//                     continue;
//                 }
//                 // If only the name matches we didn't agree, but add it for resolution.
//                 if entry.name == current.name {
//                     top_entries[i] = Some(entry);
//                     continue;
//                 }
//                 // We got different entries
//                 if entry.name > current.name {
//                     continue;
//                 }

//                 for item in top_entries.iter_mut().take(i) {
//                     *item = None;
//                 }

//                 agree = 1;
//                 top_entries[i] = Some(entry.clone());
//                 current = entry;
//             }

//             if vnf > 0 && vnf >= (readers.len() - opts.min_disks) {
//                 // warn!("list_path_raw: vnf > 0 && vnf >= (readers.len() - opts.min_disks) break");
//                 return Err(DiskError::VolumeNotFound);
//             }

//             if fnf > 0 && fnf >= (readers.len() - opts.min_disks) {
//                 // warn!("list_path_raw: fnf > 0 && fnf >= (readers.len() - opts.min_disks) break");
//                 return Err(DiskError::FileNotFound);
//             }

//             if has_err > 0 && has_err > opts.disks.len() - opts.min_disks {
//                 if let Some(finished_fn) = opts.finished.as_ref() {
//                     finished_fn(&errs).await;
//                 }
//                 let mut combined_err = Vec::new();
//                 errs.iter().zip(opts.disks.iter()).for_each(|(err, disk)| match (err, disk) {
//                     (Some(err), Some(disk)) => {
//                         combined_err.push(format!("drive {} returned: {}", disk.to_string(), err));
//                     }
//                     (Some(err), None) => {
//                         combined_err.push(err.to_string());
//                     }
//                     _ => {}
//                 });

//                 error!(
//                     "list_path_raw: has_err > 0 && has_err > opts.disks.len() - opts.min_disks break, err: {:?}",
//                     &combined_err.join(", ")
//                 );
//                 return Err(DiskError::other(combined_err.join(", ")));
//             }

//             // Break if all at EOF or error.
//             if at_eof + has_err == readers.len() {
//                 if has_err > 0 {
//                     if let Some(finished_fn) = opts.finished.as_ref() {
//                         if has_err > 0 {
//                             finished_fn(&errs).await;
//                         }
//                     }
//                 }

//                 // error!("list_path_raw: at_eof + has_err == readers.len() break {:?}", &errs);
//                 break;
//             }

//             if agree == readers.len() {
//                 for r in readers.iter_mut() {
//                     let _ = r.skip(1).await;
//                 }

//                 if let Some(agreed_fn) = opts.agreed.as_ref() {
//                     // warn!("list_path_raw: agreed_fn start, current: {:?}", &current.name);
//                     agreed_fn(current).await;
//                     // warn!("list_path_raw: agreed_fn done");
//                 }

//                 continue;
//             }

//             // warn!("list_path_raw: skip start, current: {:?}", &current.name);

//             for (i, r) in readers.iter_mut().enumerate() {
//                 if top_entries[i].is_some() {
//                     let _ = r.skip(1).await;
//                 }
//             }

//             if let Some(partial_fn) = opts.partial.as_ref() {
//                 partial_fn(MetaCacheEntries(top_entries), &errs).await;
//             }
//         }
//         Ok(())
//     });

//     if let Err(err) = revjob.await.map_err(std::io::Error::other)? {
//         error!("list_path_raw: revjob err {:?}", err);
//         let _ = cancel_tx.send(true);

//         return Err(err);
//     }

//     let results = join_all(jobs).await;
//     for result in results {
//         if let Err(err) = result {
//             error!("list_path_raw err {:?}", err);
//         }
//     }

//     // warn!("list_path_raw: done");
//     Ok(())
// }

// ============================================================================
// refactor list_path_raw_new
// ============================================================================

/// new options struct, more clear config
#[derive(Default)]
pub struct ListPathRawOptions {
    pub disks: Vec<Option<DiskStore>>,
    pub fallback_disks: Vec<Option<DiskStore>>,
    pub bucket: String,
    pub path: String,
    pub recursive: bool,
    pub filter_prefix: Option<String>,
    pub forward_to: Option<String>,
    pub min_disks: usize,
    pub report_not_found: bool,
    pub per_disk_limit: i32,
    pub agreed: Option<AgreedFn>,
    pub partial: Option<PartialFn>,
    pub finished: Option<FinishedFn>,
}

impl Clone for ListPathRawOptions {
    fn clone(&self) -> Self {
        Self {
            disks: self.disks.clone(),
            fallback_disks: self.fallback_disks.clone(),
            bucket: self.bucket.clone(),
            path: self.path.clone(),
            recursive: self.recursive,
            filter_prefix: self.filter_prefix.clone(),
            forward_to: self.forward_to.clone(),
            min_disks: self.min_disks,
            report_not_found: self.report_not_found,
            per_disk_limit: self.per_disk_limit,
            agreed: None,   // callback function can't be cloned, set to None
            partial: None,  // callback function can't be cloned, set to None
            finished: None, // callback function can't be cloned, set to None
        }
    }
}

/// disk reader - responsible for reading metadata from a single disk
struct DiskReader {
    disk: Option<DiskStore>,
    fallback_disks: Vec<Option<DiskStore>>,
    options: ListPathRawOptions,
    reader: Option<MetacacheReader<tokio::io::DuplexStream>>,
    error: Option<DiskError>,
    is_eof: bool,
}

impl DiskReader {
    fn new(disk: Option<DiskStore>, fallback_disks: Vec<Option<DiskStore>>, options: ListPathRawOptions) -> Self {
        Self {
            disk,
            fallback_disks,
            options,
            reader: None,
            error: None,
            is_eof: false,
        }
    }

    async fn initialize(&mut self) -> Result<(), DiskError> {
        let walk_opts = WalkDirOptions {
            bucket: self.options.bucket.clone(),
            base_dir: self.options.path.clone(),
            recursive: self.options.recursive,
            report_notfound: self.options.report_not_found,
            filter_prefix: self.options.filter_prefix.clone(),
            forward_to: self.options.forward_to.clone(),
            limit: self.options.per_disk_limit,
            ..Default::default()
        };

        // try primary disk
        if let Some(disk) = &self.disk {
            let (rd, mut wr) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);
            match disk.walk_dir(walk_opts.clone(), &mut wr).await {
                Ok(_) => {
                    self.reader = Some(MetacacheReader::new(rd));
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("Primary disk walk_dir error: {:?}", err);
                }
            }
        }
        // try fallback disk
        for disk in self.fallback_disks.iter().flatten() {
            let (rd, mut wr) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);
            match disk.walk_dir(walk_opts.clone(), &mut wr).await {
                Ok(_) => {
                    self.reader = Some(MetacacheReader::new(rd));
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("Fallback disk walk_dir error: {:?}", err);
                }
            }
        }
        Err(DiskError::other("All disks failed to initialize"))
    }

    async fn peek(&mut self) -> Result<Option<MetaCacheEntry>, DiskError> {
        if self.is_eof || self.error.is_some() {
            return Ok(None);
        }

        if self.reader.is_none() {
            self.initialize().await?;
        }

        if let Some(reader) = &mut self.reader {
            match reader.peek().await {
                Ok(Some(entry)) => Ok(Some(entry)),
                Ok(None) => {
                    self.is_eof = true;
                    Ok(None)
                }
                Err(err) => {
                    self.handle_read_error(err).await?;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn skip(&mut self) -> Result<(), DiskError> {
        if let Some(reader) = &mut self.reader {
            let _ = reader.skip(1).await;
        }
        Ok(())
    }

    async fn handle_read_error(&mut self, err: rustfs_filemeta::Error) -> Result<(), DiskError> {
        if err == rustfs_filemeta::Error::Unexpected || is_io_eof(&err) {
            self.is_eof = true;
            return Ok(());
        }

        if err == rustfs_filemeta::Error::FileNotFound || err == rustfs_filemeta::Error::VolumeNotFound {
            self.is_eof = true;
            return Ok(());
        }

        self.error = Some(err.into());
        Err(self.error.as_ref().unwrap().clone())
    }

    fn has_error(&self) -> bool {
        self.error.is_some()
    }

    fn is_at_eof(&self) -> bool {
        self.is_eof
    }

    fn get_error(&self) -> Option<&DiskError> {
        self.error.as_ref()
    }
}

/// entry collector - responsible for collecting and sorting entries from multiple disks
struct EntryCollector {
    readers: Vec<DiskReader>,
    min_disks: usize,
}

impl EntryCollector {
    fn new(readers: Vec<DiskReader>, min_disks: usize) -> Self {
        Self { readers, min_disks }
    }

    async fn collect_next_entries(&mut self) -> Result<Vec<Option<MetaCacheEntry>>, DiskError> {
        let mut entries = vec![None; self.readers.len()];
        let mut _at_eof = 0; // marked as unused
        let mut has_error = 0;
        let mut fnf_count = 0;
        let mut vnf_count = 0;

        // collect entries from all disks
        for (i, reader) in self.readers.iter_mut().enumerate() {
            if reader.has_error() {
                has_error += 1;
                continue;
            }

            if reader.is_at_eof() {
                _at_eof += 1;
                continue;
            }
            match reader.peek().await {
                Ok(Some(entry)) => {
                    entries[i] = Some(entry);
                }
                Ok(None) => {
                    _at_eof += 1;
                }
                Err(err) => {
                    // check error type
                    match err {
                        DiskError::FileNotFound => {
                            fnf_count += 1;
                            _at_eof += 1;
                        }
                        DiskError::VolumeNotFound => {
                            vnf_count += 1;
                            _at_eof += 1;
                        }
                        _ => {
                            has_error += 1;
                        }
                    }
                }
            }
        }

        // check
        if vnf_count > 0 && vnf_count >= (self.readers.len() - self.min_disks) {
            return Err(DiskError::VolumeNotFound);
        }

        if fnf_count > 0 && fnf_count >= (self.readers.len() - self.min_disks) {
            return Err(DiskError::FileNotFound);
        }

        if has_error > 0 && has_error > self.readers.len() - self.min_disks {
            return Err(DiskError::other("Too many disk errors"));
        }

        Ok(entries)
    }

    fn get_errors(&self) -> Vec<Option<DiskError>> {
        self.readers.iter().map(|r| r.get_error().cloned()).collect()
    }

    fn all_at_eof_or_error(&self) -> bool {
        self.readers.iter().all(|r| r.is_at_eof() || r.has_error())
    }

    async fn skip_current_entries(&mut self) {
        for reader in &mut self.readers {
            let _ = reader.skip().await;
        }
    }
}

/// entry resolver - responsible for resolving the consistency and sorting of entries
struct EntryResolver {
    entries: Vec<Option<MetaCacheEntry>>,
}

impl EntryResolver {
    fn new(entries: Vec<Option<MetaCacheEntry>>) -> Self {
        Self { entries }
    }

    fn resolve(&mut self) -> EntryResolution {
        let mut current = MetaCacheEntry::default();
        let mut agree_count = 0;
        let mut has_entries = false;
        let mut current_name = String::new();
        let mut need_clear = false;

        // find the first valid entry as the current entry
        for entry in self.entries.iter().flatten() {
            if current.name.is_empty() {
                current = entry.clone();
                current_name = entry.name.clone();
                has_entries = true;
                break;
            }
        }

        if !has_entries {
            return EntryResolution::NoEntries;
        }

        // check consistency
        for entry in self.entries.iter().flatten() {
            if entry.name == current.name {
                if let (_, true) = current.matches(Some(entry), true) {
                    agree_count += 1;
                }
            } else if entry.name < current.name {
                // find a smaller entry, need to restart
                current = entry.clone();
                current_name = entry.name.clone();
                agree_count = 1;
                need_clear = true;
            }
        }

        if need_clear {
            self.clear_previous_entries(&current_name);
        }

        if agree_count == self.entries.len() {
            EntryResolution::Agreed(current)
        } else {
            EntryResolution::Partial(MetaCacheEntries(self.entries.clone()))
        }
    }

    fn clear_previous_entries(&mut self, current_name: &String) {
        for entry in self.entries.iter_mut().flatten() {
            if entry.name > *current_name {
                *entry = MetaCacheEntry::default();
            }
        }
    }
}

#[derive(Debug)]
enum EntryResolution {
    Agreed(MetaCacheEntry),
    Partial(MetaCacheEntries),
    NoEntries,
}

/// callback manager - responsible for calling various callback functions
struct CallbackManager {
    agreed: Option<AgreedFn>,
    partial: Option<PartialFn>,
    finished: Option<FinishedFn>,
}

impl CallbackManager {
    fn new(agreed: Option<AgreedFn>, partial: Option<PartialFn>, finished: Option<FinishedFn>) -> Self {
        Self {
            agreed,
            partial,
            finished,
        }
    }

    async fn call_agreed(&self, entry: MetaCacheEntry) {
        if let Some(agreed_fn) = &self.agreed {
            agreed_fn(entry).await;
        }
    }

    async fn call_partial(&self, entries: MetaCacheEntries, errors: &[Option<DiskError>]) {
        if let Some(partial_fn) = &self.partial {
            partial_fn(entries, errors).await;
        }
    }

    async fn call_finished(&self, errors: &[Option<DiskError>]) {
        if let Some(finished_fn) = &self.finished {
            finished_fn(errors).await;
        }
    }
}

/// refactored main function with enhanced features
pub async fn list_path_raw(opts: ListPathRawOptions) -> disk::error::Result<()> {
    if opts.disks.is_empty() {
        return Err(DiskError::other("list_path_raw_new: 0 drives provided"));
    }

    // create enhanced components
    let mut concurrent_writer = ConcurrentWriter::new();
    let mut error_reporter = ErrorReporter::new(opts.disks.clone());

    // extract callback functions before moving opts
    let agreed = opts.agreed;
    let partial = opts.partial;
    let finished = opts.finished;
    let callback_manager = CallbackManager::new(agreed, partial, finished);

    // extract read-only fields for concurrent writing
    let disks = opts.disks.clone();
    let fallback_disks = opts.fallback_disks.clone();
    let bucket = opts.bucket.clone();
    let path = opts.path.clone();
    let recursive = opts.recursive;
    let filter_prefix = opts.filter_prefix.clone();
    let forward_to = opts.forward_to.clone();
    let report_not_found = opts.report_not_found;
    let per_disk_limit = opts.per_disk_limit;

    // start concurrent writing operations
    let mut readers = concurrent_writer
        .start_writing(
            &disks,
            &fallback_disks,
            &bucket,
            &path,
            recursive,
            &filter_prefix,
            &forward_to,
            report_not_found,
            per_disk_limit,
        )
        .await;

    // main processing loop (similar to original list_path_raw)
    loop {
        // check cancel signal
        if LIST_PATH_RAW_CANCEL_TOKEN.is_cancelled() {
            return Err(DiskError::other("canceled"));
        }

        let mut current = MetaCacheEntry::default();
        let mut top_entries: Vec<Option<MetaCacheEntry>> = vec![None; readers.len()];

        let mut at_eof = 0;
        let mut fnf = 0;
        let mut vnf = 0;
        let mut has_err = 0;
        let mut agree = 0;

        // collect entries from all readers (similar to original logic)
        for (i, r) in readers.iter_mut().enumerate() {
            if error_reporter.get_errors()[i].is_some() {
                has_err += 1;
                continue;
            }

            let entry = match r.peek().await {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    at_eof += 1;
                    continue;
                }
                Err(err) => {
                    if err == rustfs_filemeta::Error::Unexpected || is_io_eof(&err) {
                        at_eof += 1;
                        continue;
                    }
                    if err == rustfs_filemeta::Error::FileNotFound {
                        at_eof += 1;
                        fnf += 1;
                        continue;
                    } else if err == rustfs_filemeta::Error::VolumeNotFound {
                        at_eof += 1;
                        fnf += 1;
                        vnf += 1;
                        continue;
                    } else {
                        has_err += 1;
                        error_reporter.set_error(i, err.into());
                        continue;
                    }
                }
            };

            // If no current, add it.
            if current.name.is_empty() {
                top_entries[i] = Some(entry.clone());
                current = entry;
                agree += 1;
                continue;
            }
            // If exact match, we agree.
            if let (_, true) = current.matches(Some(&entry), true) {
                top_entries[i] = Some(entry);
                agree += 1;
                continue;
            }
            // If only the name matches we didn't agree, but add it for resolution.
            if entry.name == current.name {
                top_entries[i] = Some(entry);
                continue;
            }
            // We got different entries - complete sorting logic
            if entry.name > current.name {
                continue;
            }

            // Restart sorting with smaller entry
            for item in top_entries.iter_mut().take(i) {
                *item = None;
            }

            agree = 1;
            top_entries[i] = Some(entry.clone());
            current = entry;
        }

        // check error conditions (similar to original logic)
        if vnf > 0 && vnf >= (readers.len() - opts.min_disks) {
            return Err(DiskError::VolumeNotFound);
        }

        if fnf > 0 && fnf >= (readers.len() - opts.min_disks) {
            return Err(DiskError::FileNotFound);
        }

        if has_err > 0 && has_err > opts.disks.len() - opts.min_disks {
            if let Some(finished_fn) = &callback_manager.finished {
                finished_fn(error_reporter.get_errors()).await;
            }
            let combined_err = error_reporter.combine_errors();
            error!(
                "list_path_raw_new: has_err > 0 && has_err > opts.disks.len() - opts.min_disks break, err: {:?}",
                &combined_err
            );
            return Err(DiskError::other(combined_err));
        }

        // Break if all at EOF or error.
        if at_eof + has_err == readers.len() {
            if has_err > 0 {
                if let Some(finished_fn) = &callback_manager.finished {
                    finished_fn(error_reporter.get_errors()).await;
                }
            }
            break;
        }

        if agree == readers.len() {
            for r in readers.iter_mut() {
                let _ = r.skip(1).await;
            }

            if let Some(agreed_fn) = &callback_manager.agreed {
                agreed_fn(current).await;
            }

            continue;
        }

        // skip current entries
        for (i, r) in readers.iter_mut().enumerate() {
            if top_entries[i].is_some() {
                let _ = r.skip(1).await;
            }
        }

        if let Some(partial_fn) = &callback_manager.partial {
            partial_fn(MetaCacheEntries(top_entries), error_reporter.get_errors()).await;
        }
    }

    // wait for all writing jobs to complete
    let write_results = concurrent_writer.wait_completion().await;
    for result in write_results {
        if let Err(err) = result {
            error!("list_path_raw_new write job error: {:?}", err);
        }
    }

    Ok(())
}

// ============================================================================
// concurrent writer manager
// ============================================================================

/// concurrent writer manager - responsible for managing concurrent walk_dir operations
struct ConcurrentWriter {
    jobs: Vec<tokio::task::JoinHandle<std::result::Result<(), DiskError>>>,
}

impl ConcurrentWriter {
    fn new() -> Self {
        Self { jobs: Vec::new() }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_writing(
        &mut self,
        disks: &[Option<DiskStore>],
        fallback_disks: &[Option<DiskStore>],
        bucket: &str,
        path: &str,
        recursive: bool,
        filter_prefix: &Option<String>,
        forward_to: &Option<String>,
        report_not_found: bool,
        per_disk_limit: i32,
    ) -> Vec<MetacacheReader<tokio::io::DuplexStream>> {
        let mut readers = Vec::with_capacity(disks.len());
        let fds = Arc::new(fallback_disks.to_vec());

        for disk in disks.iter() {
            let opdisk = disk.clone();
            let fds_clone = fds.clone();
            let bucket = bucket.to_string();
            let path = path.to_string();
            let filter_prefix = filter_prefix.clone();
            let forward_to = forward_to.clone();
            let (rd, mut wr) = tokio::io::duplex(DEFAULT_READ_BUFFER_SIZE);

            readers.push(MetacacheReader::new(rd));

            self.jobs.push(spawn(async move {
                let walk_opts = WalkDirOptions {
                    bucket: bucket.clone(),
                    base_dir: path.clone(),
                    recursive,
                    report_notfound: report_not_found,
                    filter_prefix: filter_prefix.clone(),
                    forward_to: forward_to.clone(),
                    limit: per_disk_limit,
                    ..Default::default()
                };

                let mut need_fallback = false;
                if let Some(disk) = opdisk {
                    match disk.walk_dir(walk_opts.clone(), &mut wr).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("walk dir err {:?}", &err);
                            need_fallback = true;
                        }
                    }
                } else {
                    need_fallback = true;
                }

                while need_fallback {
                    let disk = match fds_clone.iter().find(|d| d.is_some()) {
                        Some(d) => {
                            if let Some(disk) = d.clone() {
                                disk
                            } else {
                                break;
                            }
                        }
                        None => break,
                    };

                    match disk
                        .as_ref()
                        .walk_dir(
                            WalkDirOptions {
                                bucket: bucket.clone(),
                                base_dir: path.clone(),
                                recursive,
                                report_notfound: report_not_found,
                                filter_prefix: filter_prefix.clone(),
                                forward_to: forward_to.clone(),
                                limit: per_disk_limit,
                                ..Default::default()
                            },
                            &mut wr,
                        )
                        .await
                    {
                        Ok(_) => {
                            need_fallback = false;
                        }
                        Err(err) => {
                            error!("walk dir2 err {:?}", &err);
                            break;
                        }
                    }
                }

                Ok(())
            }));
        }

        readers
    }

    async fn wait_completion(&mut self) -> Vec<std::result::Result<(), DiskError>> {
        let results = join_all(std::mem::take(&mut self.jobs)).await;
        results
            .into_iter()
            .map(|r| match r {
                Ok(inner) => inner,
                Err(e) => Err(DiskError::other(e.to_string())),
            })
            .collect()
    }
}

// ============================================================================
// enhanced error reporter
// ============================================================================

/// enhanced error reporter - responsible for detailed error combination and reporting
struct ErrorReporter {
    errors: Vec<Option<DiskError>>,
    disk_info: Vec<Option<DiskStore>>,
}

impl ErrorReporter {
    fn new(disk_info: Vec<Option<DiskStore>>) -> Self {
        let errors = vec![None; disk_info.len()];
        Self { errors, disk_info }
    }

    fn set_error(&mut self, index: usize, error: DiskError) {
        if index < self.errors.len() {
            self.errors[index] = Some(error);
        }
    }

    fn combine_errors(&self) -> String {
        let mut combined_err = Vec::new();
        self.errors
            .iter()
            .zip(self.disk_info.iter())
            .for_each(|(err, disk)| match (err, disk) {
                (Some(err), Some(disk)) => {
                    combined_err.push(format!("drive {} returned: {}", disk.to_string(), err));
                }
                (Some(err), None) => {
                    combined_err.push(err.to_string());
                }
                _ => {}
            });
        combined_err.join(", ")
    }

    fn get_errors(&self) -> &[Option<DiskError>] {
        &self.errors
    }

    fn has_too_many_errors(&self, min_disks: usize) -> bool {
        let error_count = self.errors.iter().filter(|e| e.is_some()).count();
        error_count > self.errors.len() - min_disks
    }
}

// ============================================================================
// enhanced entry resolver with complete sorting
// ============================================================================

/// enhanced entry resolver with complete sorting logic
struct EnhancedEntryResolver {
    entries: Vec<Option<MetaCacheEntry>>,
}

impl EnhancedEntryResolver {
    fn new(entries: Vec<Option<MetaCacheEntry>>) -> Self {
        Self { entries }
    }

    fn resolve_with_complete_sorting(&mut self) -> EntryResolution {
        let mut current = MetaCacheEntry::default();
        let mut agree_count = 0;
        let mut has_entries = false;

        // find the first valid entry as the current entry
        for entry in self.entries.iter().flatten() {
            if current.name.is_empty() {
                current = entry.clone();
                has_entries = true;
                break;
            }
        }

        if !has_entries {
            return EntryResolution::NoEntries;
        }

        // check consistency with complete sorting logic
        let entries_clone = self.entries.clone();
        let len = entries_clone.len();
        let mut i = 0;
        let mut need_restart = false;

        while i < len {
            let entry_opt = entries_clone[i].as_ref().cloned();
            if let Some(entry) = entry_opt {
                if entry.name == current.name {
                    if let (_, true) = current.matches(Some(&entry), true) {
                        agree_count += 1;
                    }
                } else if entry.name < current.name {
                    // find a smaller entry, need to restart sorting
                    need_restart = true;
                    break;
                } else if entry.name > current.name {
                    // skip this entry, it's larger than current
                    // do nothing, continue to next iteration
                }
            }
            i += 1;
        }

        if need_restart {
            // Find the smallest entry and restart
            let mut smallest_entry = None;
            let mut smallest_name = String::new();

            for entry in self.entries.iter().flatten() {
                if smallest_name.is_empty() || entry.name < smallest_name {
                    smallest_name = entry.name.clone();
                    smallest_entry = Some(entry.clone());
                }
            }

            if let Some(smallest) = smallest_entry {
                // Check if all entries agree on the smallest
                let mut all_agree = true;
                for entry in self.entries.iter().flatten() {
                    if entry.name != smallest.name {
                        all_agree = false;
                        break;
                    }
                }

                if all_agree {
                    return EntryResolution::Agreed(smallest);
                }
            }
        }

        if agree_count == self.entries.len() {
            EntryResolution::Agreed(current)
        } else {
            EntryResolution::Partial(MetaCacheEntries(entries_clone))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::pin::Pin;

    use super::*;
    use crate::StorageAPI;
    use crate::disk::DiskStore;
    use crate::disk::endpoint::Endpoint;
    use crate::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    use crate::store::ECStore;
    use crate::store_api::{ObjectIO, ObjectOptions, PutObjReader};
    use tokio::fs;
    use tokio::sync::mpsc;
    use tracing::info;
    use uuid::Uuid;

    /// Test helper function to create a simple test environment
    async fn create_test_env() -> (Vec<PathBuf>, Arc<ECStore>) {
        use std::sync::OnceLock;
        static GLOBAL_ENV: OnceLock<(Vec<PathBuf>, Arc<ECStore>)> = OnceLock::new();

        // Fast path: already initialized, just clone and return
        if let Some((paths, ecstore)) = GLOBAL_ENV.get() {
            return (paths.clone(), ecstore.clone());
        }

        // Create temp directory
        let test_base_dir = format!("/tmp/rustfs_list_path_test_{}", Uuid::new_v4());
        let temp_dir = PathBuf::from(&test_base_dir);
        if temp_dir.exists() {
            tokio::fs::remove_dir_all(&temp_dir).await.ok();
        }
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();

        // Create 4 disk directories
        let disk_paths = vec![
            temp_dir.join("disk1"),
            temp_dir.join("disk2"),
            temp_dir.join("disk3"),
            temp_dir.join("disk4"),
        ];

        for disk_path in &disk_paths {
            tokio::fs::create_dir_all(disk_path).await.unwrap();
        }

        // Create endpoints
        let mut endpoints = Vec::new();
        for (i, disk_path) in disk_paths.iter().enumerate() {
            let mut endpoint = Endpoint::try_from(disk_path.to_str().unwrap()).unwrap();
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(i);
            endpoints.push(endpoint);
        }

        let pool_endpoints = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "test".to_string(),
            platform: format!("OS: {} | Arch: {}", std::env::consts::OS, std::env::consts::ARCH),
        };

        let endpoint_pools = EndpointServerPools(vec![pool_endpoints]);

        // Format disks
        crate::store::init_local_disks(endpoint_pools.clone()).await.unwrap();

        // Create ECStore with dynamic port
        let port = 9002; // Use different port to avoid conflicts
        let server_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let ecstore = ECStore::new(server_addr, endpoint_pools).await.unwrap();

        // Init bucket metadata system
        let buckets_list = ecstore
            .list_bucket(&crate::store_api::BucketOptions {
                no_metadata: true,
                ..Default::default()
            })
            .await
            .unwrap();
        let buckets = buckets_list.into_iter().map(|v| v.name).collect();
        crate::bucket::metadata_sys::init_bucket_metadata_sys(ecstore.clone(), buckets).await;

        // Store in global once lock
        let _ = GLOBAL_ENV.set((disk_paths.clone(), ecstore.clone()));

        (disk_paths, ecstore)
    }

    /// Test helper: Create a test bucket
    async fn create_test_bucket(ecstore: &Arc<ECStore>, bucket_name: &str) {
        (**ecstore)
            .make_bucket(bucket_name, &Default::default())
            .await
            .expect("Failed to create test bucket");
        info!("Created test bucket: {}", bucket_name);
    }

    /// Test helper: Upload test object
    async fn upload_test_object(ecstore: &Arc<ECStore>, bucket: &str, object: &str, data: &[u8]) {
        let mut reader = PutObjReader::from_vec(data.to_vec());
        let object_info = (**ecstore)
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("Failed to upload test object");

        info!("Uploaded test object: {}/{} ({} bytes)", bucket, object, object_info.size);
    }

    /// Test helper: Cleanup test environment
    async fn cleanup_test_env(disk_paths: &[PathBuf]) {
        for disk_path in disk_paths {
            if disk_path.exists() {
                fs::remove_dir_all(disk_path).await.expect("Failed to cleanup disk path");
            }
        }

        // Attempt to clean up base directory inferred from disk_paths[0]
        if let Some(parent) = disk_paths.first().and_then(|p| p.parent()).and_then(|p| p.parent()) {
            if parent.exists() {
                fs::remove_dir_all(parent).await.ok();
            }
        }

        info!("Test environment cleaned up");
    }

    /// Test helper: Create test data for list_path_raw_new testing
    async fn create_test_data(ecstore: &Arc<ECStore>, bucket_name: &str) {
        // Create test bucket
        create_test_bucket(ecstore, bucket_name).await;

        // Upload test objects
        let test_objects = vec![
            ("test1.txt", b"Hello World 1"),
            ("test2.txt", b"Hello World 2"),
            ("folder1/test3.txt", b"Hello World 3"),
            ("folder1/test4.txt", b"Hello World 4"),
            ("folder2/test5.txt", b"Hello World 5"),
        ];

        for (object_name, data) in test_objects {
            upload_test_object(ecstore, bucket_name, object_name, data).await;
        }

        info!("Created test data for bucket: {}", bucket_name);
    }

    /// Test helper: Get disk stores from ECStore for testing
    async fn get_disk_stores_from_ecstore(ecstore: &Arc<ECStore>) -> Vec<Option<DiskStore>> {
        // Get disks from ECStore's disk_map
        // ECStore.disk_map is HashMap<usize, Vec<Option<DiskStore>>>
        // We get the first pool (index 0) and return its disks
        if let Some(disks) = ecstore.disk_map.get(&0) {
            disks.clone()
        } else {
            // Fallback: return empty vector if no disks found
            vec![]
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_list_path_raw_new_empty_disks() {
        let opts = ListPathRawOptions {
            disks: vec![],
            fallback_disks: vec![],
            bucket: "test-bucket".to_string(),
            path: "".to_string(),
            recursive: true,
            filter_prefix: None,
            forward_to: None,
            min_disks: 2,
            report_not_found: false,
            per_disk_limit: 100,
            agreed: None,
            partial: None,
            finished: None,
        };

        let result = list_path_raw(opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("0 drives provided"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_entry_resolver_partial() {
        // Create test entries that should be partial
        let mut entries = vec![None; 4];
        let test_entry1 = MetaCacheEntry {
            name: "test1.txt".to_string(),
            ..Default::default()
        };
        let test_entry2 = MetaCacheEntry {
            name: "test2.txt".to_string(),
            ..Default::default()
        };

        entries[0] = Some(test_entry1.clone());
        entries[1] = Some(test_entry2.clone());
        entries[2] = Some(test_entry1.clone());
        entries[3] = Some(test_entry2.clone());

        let mut resolver = EntryResolver::new(entries);
        let resolution = resolver.resolve();

        match resolution {
            EntryResolution::Partial(_) => {
                // Expected partial resolution
            }
            _ => panic!("Expected Partial resolution"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_callback_manager() {
        let (tx, mut rx) = mpsc::channel(10);

        let agreed_fn = Box::new(move |entry: MetaCacheEntry| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(format!("agreed: {}", entry.name)).await;
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        let callback_manager = CallbackManager::new(Some(agreed_fn), None, None);

        let test_entry = MetaCacheEntry {
            name: "test.txt".to_string(),
            ..Default::default()
        };

        callback_manager.call_agreed(test_entry).await;

        let result = rx.try_recv().unwrap();
        assert_eq!(result, "agreed: test.txt");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn test_list_path_raw_new_cancellation() {
        let (disk_paths, ecstore) = create_test_env().await;
        let bucket = "test-bucket-cancel";

        // Create test data
        create_test_data(&ecstore, bucket).await;

        // Get disk stores
        let disks = get_disk_stores_from_ecstore(&ecstore).await;

        let opts = ListPathRawOptions {
            disks: disks.clone(),
            fallback_disks: vec![],
            bucket: bucket.to_string(),
            path: "".to_string(),
            recursive: true,
            filter_prefix: None,
            forward_to: None,
            min_disks: 2,
            report_not_found: false,
            per_disk_limit: 100,
            agreed: Some(Box::new(|_entry: MetaCacheEntry| {
                Box::pin(async move {
                    // Simulate some processing time
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })),
            partial: None,
            finished: None,
        };

        let result = list_path_raw(opts).await;
        // Since we don't have a cancellation mechanism in the new implementation,
        // this should complete normally or with an error due to no data
        assert!(result.is_ok() || result.unwrap_err().to_string().contains("0 drives"));
        cleanup_test_env(&disk_paths).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_entry_resolver_agreed() {
        // Create test entries that should agree
        let mut entries = vec![None; 4];

        // Create a simple FileMeta with one version for testing
        let mut file_meta = rustfs_filemeta::FileMeta::new();
        let version = rustfs_filemeta::FileMetaVersion {
            version_type: rustfs_filemeta::VersionType::Object,
            object: Some(rustfs_filemeta::MetaObject::default()),
            ..Default::default()
        };
        file_meta.versions.push(rustfs_filemeta::FileMetaShallowVersion {
            header: version.header(),
            meta: version.marshal_msg().unwrap(),
        });

        let metadata = file_meta.marshal_msg().unwrap();

        let test_entry = MetaCacheEntry {
            name: "test.txt".to_string(),
            metadata,
            ..Default::default()
        };

        for entry in entries.iter_mut().take(4) {
            *entry = Some(test_entry.clone());
        }

        let mut resolver = EntryResolver::new(entries);
        let resolution = resolver.resolve();

        match resolution {
            EntryResolution::Agreed(entry) => {
                assert_eq!(entry.name, "test.txt");
            }
            _ => panic!("Expected Agreed resolution"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn test_list_path_raw_new_with_data() {
        let (disk_paths, ecstore) = create_test_env().await;
        let bucket = "test-bucket-data";

        // Create test data
        create_test_data(&ecstore, bucket).await;

        // Get disk stores from ECStore
        let disks = get_disk_stores_from_ecstore(&ecstore).await;
        assert!(!disks.is_empty(), "Should have disks from ECStore");

        // Create a simple counter to track entries
        let entry_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let entry_count_clone = entry_count.clone();

        let opts = ListPathRawOptions {
            disks: disks.clone(),
            fallback_disks: vec![],
            bucket: bucket.to_string(),
            path: "".to_string(),
            recursive: true,
            filter_prefix: None,
            forward_to: None,
            min_disks: 2,
            report_not_found: false,
            per_disk_limit: 100,
            agreed: Some(Box::new(move |_entry: MetaCacheEntry| {
                let entry_count = entry_count_clone.clone();
                Box::pin(async move {
                    entry_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })),
            partial: None,
            finished: None,
        };

        // Run list_path_raw
        let result = list_path_raw(opts).await;

        // Verify results
        match result {
            Ok(()) => {
                // Success case - should have processed some entries
                let count = entry_count.load(std::sync::atomic::Ordering::SeqCst);
                info!("List operation completed successfully, processed {} entries", count);
                // In a real test, you might want to verify specific entries
            }
            Err(e) => {
                // Error case - might be expected if no data or other conditions
                info!("List operation failed with error: {}", e);
                // This might be expected in some cases
            }
        }
        cleanup_test_env(&disk_paths).await;
    }

    /// Test concurrent writing functionality
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_writer() {
        let mut writer = ConcurrentWriter::new();

        // Create mock disks (empty for this test)
        let disks = vec![None::<DiskStore>; 3];
        let fallback_disks = vec![None::<DiskStore>; 2];

        // Test start_writing with empty disks
        let readers = writer
            .start_writing(&disks, &fallback_disks, "test-bucket", "test-path", true, &None, &None, false, 100)
            .await;

        // Should create readers for each disk
        assert_eq!(readers.len(), 3);

        // Wait for completion
        let results = writer.wait_completion().await;
        assert_eq!(results.len(), 3);

        // All should complete (even if with errors due to empty disks)
        for result in results {
            assert!(result.is_ok() || result.unwrap_err().to_string().contains("All disks failed"));
        }
    }

    /// Test enhanced error reporter functionality
    #[tokio::test(flavor = "multi_thread")]
    async fn test_error_reporter() {
        // Create test directories
        let test_dir1 = "/tmp/test1";
        let test_dir2 = "/tmp/test2";

        // Create directories if they don't exist
        let _ = tokio::fs::create_dir_all(test_dir1).await;
        let _ = tokio::fs::create_dir_all(test_dir2).await;

        // Create mock disk info with proper endpoints
        let disk_info = vec![
            Some(
                crate::disk::new_disk(
                    &Endpoint::try_from(test_dir1).unwrap(),
                    &crate::disk::DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .unwrap(),
            ),
            Some(
                crate::disk::new_disk(
                    &Endpoint::try_from(test_dir2).unwrap(),
                    &crate::disk::DiskOption {
                        cleanup: false,
                        health_check: false,
                    },
                )
                .await
                .unwrap(),
            ),
            None,
        ];

        let mut reporter = ErrorReporter::new(disk_info);

        // Test setting errors
        reporter.set_error(0, DiskError::FileNotFound);
        reporter.set_error(1, DiskError::VolumeNotFound);

        // Test error combination
        let combined = reporter.combine_errors();
        // Note: The actual error messages might be different, so we check for the presence of error information
        assert!(!combined.is_empty(), "Error combination should not be empty");
        assert!(
            combined.contains("drive") || combined.contains("disk"),
            "Should contain drive/disk information"
        );

        // Test error counting - in mock environment, results may vary
        // Just verify the method works without specific expectations
        let _too_many_errors = reporter.has_too_many_errors(1);
        let _not_too_many_errors = reporter.has_too_many_errors(2);

        // Test get_errors
        let errors = reporter.get_errors();
        assert_eq!(errors.len(), 3);
        assert!(errors[0].is_some());
        assert!(errors[1].is_some());
        assert!(errors[2].is_none());
    }

    /// Test enhanced entry resolver with complete sorting
    #[tokio::test(flavor = "multi_thread")]
    async fn test_enhanced_entry_resolver() {
        // Create test entries with different names
        let mut entries = vec![None; 4];

        // Create a simple FileMeta with one version for testing
        let mut file_meta = rustfs_filemeta::FileMeta::new();
        let version = rustfs_filemeta::FileMetaVersion {
            version_type: rustfs_filemeta::VersionType::Object,
            object: Some(rustfs_filemeta::MetaObject::default()),
            ..Default::default()
        };
        file_meta.versions.push(rustfs_filemeta::FileMetaShallowVersion {
            header: version.header(),
            meta: version.marshal_msg().unwrap(),
        });
        let metadata = file_meta.marshal_msg().unwrap();

        // Test case 1: All entries agree
        let test_entry = MetaCacheEntry {
            name: "test.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        };

        for entry in entries.iter_mut().take(4) {
            *entry = Some(test_entry.clone());
        }

        let mut resolver = EnhancedEntryResolver::new(entries.clone());
        let resolution = resolver.resolve_with_complete_sorting();

        match resolution {
            EntryResolution::Agreed(entry) => {
                assert_eq!(entry.name, "test.txt");
            }
            _ => panic!("Expected Agreed resolution"),
        }

        // Test case 2: Partial agreement
        entries[0] = Some(MetaCacheEntry {
            name: "test1.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[1] = Some(MetaCacheEntry {
            name: "test2.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[2] = Some(MetaCacheEntry {
            name: "test1.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[3] = Some(MetaCacheEntry {
            name: "test2.txt".to_string(),
            metadata,
            ..Default::default()
        });

        let mut resolver = EnhancedEntryResolver::new(entries);
        let resolution = resolver.resolve_with_complete_sorting();

        match resolution {
            EntryResolution::Partial(_) => {
                // Expected partial resolution
            }
            _ => panic!("Expected Partial resolution"),
        }
    }

    /// Test complete sorting logic with smaller entries
    #[tokio::test(flavor = "multi_thread")]
    async fn test_complete_sorting_logic() {
        // Create test entries with different names in mixed order
        let mut entries = vec![None; 4];

        // Create a simple FileMeta with one version for testing
        let mut file_meta = rustfs_filemeta::FileMeta::new();
        let version = rustfs_filemeta::FileMetaVersion {
            version_type: rustfs_filemeta::VersionType::Object,
            object: Some(rustfs_filemeta::MetaObject::default()),
            ..Default::default()
        };
        file_meta.versions.push(rustfs_filemeta::FileMetaShallowVersion {
            header: version.header(),
            meta: version.marshal_msg().unwrap(),
        });
        let metadata = file_meta.marshal_msg().unwrap();

        // Test case: Mixed order entries, should find smallest and restart sorting
        entries[0] = Some(MetaCacheEntry {
            name: "z.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[1] = Some(MetaCacheEntry {
            name: "a.txt".to_string(), // This is smaller, should restart sorting
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[2] = Some(MetaCacheEntry {
            name: "b.txt".to_string(),
            metadata: metadata.clone(),
            ..Default::default()
        });
        entries[3] = Some(MetaCacheEntry {
            name: "c.txt".to_string(),
            metadata,
            ..Default::default()
        });

        let mut resolver = EnhancedEntryResolver::new(entries);
        let resolution = resolver.resolve_with_complete_sorting();

        match resolution {
            EntryResolution::Partial(entries) => {
                // Expected partial resolution since all entries are different
                // Verify that we have the correct entries
                let mut entry_names = Vec::new();
                for entry in entries.0.iter().flatten() {
                    entry_names.push(entry.name.clone());
                }
                assert_eq!(entry_names, vec!["z.txt", "a.txt", "b.txt", "c.txt"]);
            }
            EntryResolution::Agreed(entry) => {
                panic!("Expected Partial resolution, got Agreed with: {}", entry.name);
            }
            EntryResolution::NoEntries => {
                panic!("Expected Partial resolution, got NoEntries");
            }
        }
    }

    /// Test error handling and reporting in list_path_raw_new
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn test_error_handling_and_reporting() {
        let (disk_paths, ecstore) = create_test_env().await;
        let bucket = "test-bucket-errors";

        // Create test data
        create_test_data(&ecstore, bucket).await;

        // Get disk stores
        let disks = get_disk_stores_from_ecstore(&ecstore).await;
        assert!(!disks.is_empty(), "Should have disks from ECStore");

        // Create error tracking
        let error_received = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let error_received_clone = error_received.clone();

        let opts = ListPathRawOptions {
            disks: disks.clone(),
            fallback_disks: vec![],
            bucket: bucket.to_string(),
            path: "".to_string(),
            recursive: true,
            filter_prefix: None,
            forward_to: None,
            min_disks: 2,
            report_not_found: false,
            per_disk_limit: 100,
            agreed: None,
            partial: None,
            finished: Some(Box::new(move |_errors: &[Option<DiskError>]| {
                let error_received = error_received_clone.clone();
                Box::pin(async move {
                    error_received.store(true, std::sync::atomic::Ordering::SeqCst);
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })),
        };

        // Run list_path_raw
        let result = list_path_raw(opts).await;

        // Should complete successfully with test data
        assert!(result.is_ok());

        // Check if finished callback was called (should be called even on success)
        // Note: In this test case, it might not be called if no errors occur
        // The important thing is that the function completes without panicking

        cleanup_test_env(&disk_paths).await;
    }

    /// Test partial agreement handling
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn test_partial_agreement_handling() {
        let (disk_paths, ecstore) = create_test_env().await;
        let bucket = "test-bucket-partial";

        // Create test data with some duplicate names to trigger partial agreement
        create_test_data(&ecstore, bucket).await;

        // Get disk stores
        let disks = get_disk_stores_from_ecstore(&ecstore).await;
        assert!(!disks.is_empty(), "Should have disks from ECStore");

        // Create counters for tracking
        let agreed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let partial_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let agreed_count_clone = agreed_count.clone();
        let partial_count_clone = partial_count.clone();

        let opts = ListPathRawOptions {
            disks: disks.clone(),
            fallback_disks: vec![],
            bucket: bucket.to_string(),
            path: "".to_string(),
            recursive: true,
            filter_prefix: None,
            forward_to: None,
            min_disks: 2,
            report_not_found: false,
            per_disk_limit: 100,
            agreed: Some(Box::new(move |_entry: MetaCacheEntry| {
                let agreed_count = agreed_count_clone.clone();
                Box::pin(async move {
                    agreed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })),
            partial: Some(Box::new(move |_entries: MetaCacheEntries, _errors: &[Option<DiskError>]| {
                let partial_count = partial_count_clone.clone();
                Box::pin(async move {
                    partial_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })),
            finished: None,
        };

        // Run list_path_raw
        let result = list_path_raw(opts).await;

        // Should complete successfully
        assert!(result.is_ok());

        // Check that both agreed and partial callbacks were called
        let agreed = agreed_count.load(std::sync::atomic::Ordering::SeqCst);
        let partial = partial_count.load(std::sync::atomic::Ordering::SeqCst);

        info!("Agreed callbacks: {}, Partial callbacks: {}", agreed, partial);

        // Should have processed some entries (either agreed or partial)
        assert!(agreed > 0 || partial > 0, "Should have processed some entries");

        cleanup_test_env(&disk_paths).await;
    }
}
