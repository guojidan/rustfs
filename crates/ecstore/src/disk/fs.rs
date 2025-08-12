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

use std::{fs::Metadata, path::Path};

use tokio::{
    fs::{self, File},
    io,
};
#[cfg(all(target_os = "linux", feature = "uring-io"))]
use tokio::task;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[cfg(not(windows))]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    if f1.dev() != f2.dev() {
        return false;
    }

    if f1.ino() != f2.ino() {
        return false;
    }

    if f1.size() != f2.size() {
        return false;
    }
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.mtime() != f2.mtime() {
        return false;
    }

    true
}

#[cfg(windows)]
pub fn same_file(f1: &Metadata, f2: &Metadata) -> bool {
    if f1.permissions() != f2.permissions() {
        return false;
    }

    if f1.file_type() != f2.file_type() {
        return false;
    }

    if f1.len() != f2.len() {
        return false;
    }
    true
}

type FileMode = usize;

pub const O_RDONLY: FileMode = 0x00000;
pub const O_WRONLY: FileMode = 0x00001;
pub const O_RDWR: FileMode = 0x00002;
pub const O_CREATE: FileMode = 0x00040;
// pub const O_EXCL: FileMode = 0x00080;
// pub const O_NOCTTY: FileMode = 0x00100;
pub const O_TRUNC: FileMode = 0x00200;
// pub const O_NONBLOCK: FileMode = 0x00800;
pub const O_APPEND: FileMode = 0x00400;
// pub const O_SYNC: FileMode = 0x01000;
// pub const O_ASYNC: FileMode = 0x02000;
// pub const O_CLOEXEC: FileMode = 0x80000;

//      read: bool,
//     write: bool,
//     append: bool,
//     truncate: bool,
//     create: bool,
//     create_new: bool,

pub async fn open_file(path: impl AsRef<Path>, mode: FileMode) -> io::Result<File> {
    let mut opts = fs::OpenOptions::new();

    match mode & (O_RDONLY | O_WRONLY | O_RDWR) {
        O_RDONLY => {
            opts.read(true);
        }
        O_WRONLY => {
            opts.write(true);
        }
        O_RDWR => {
            opts.read(true);
            opts.write(true);
        }
        _ => (),
    };

    if mode & O_CREATE != 0 {
        opts.create(true);
    }

    if mode & O_APPEND != 0 {
        opts.append(true);
    }

    if mode & O_TRUNC != 0 {
        opts.truncate(true);
    }

    opts.open(path.as_ref()).await
}

pub async fn access(path: impl AsRef<Path>) -> io::Result<()> {
    fs::metadata(path).await?;
    Ok(())
}

pub fn access_std(path: impl AsRef<Path>) -> io::Result<()> {
    tokio::task::block_in_place(|| std::fs::metadata(path))?;
    Ok(())
}

pub async fn lstat(path: impl AsRef<Path>) -> io::Result<Metadata> {
    fs::metadata(path).await
}

pub fn lstat_std(path: impl AsRef<Path>) -> io::Result<Metadata> {
    tokio::task::block_in_place(|| std::fs::metadata(path))
}

pub async fn make_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(path.as_ref()).await
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn remove(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = fs::metadata(path.as_ref()).await?;
    if meta.is_dir() {
        fs::remove_dir(path.as_ref()).await
    } else {
        fs::remove_file(path.as_ref()).await
    }
}

pub async fn remove_all(path: impl AsRef<Path>) -> io::Result<()> {
    let meta = fs::metadata(path.as_ref()).await?;
    if meta.is_dir() {
        fs::remove_dir_all(path.as_ref()).await
    } else {
        fs::remove_file(path.as_ref()).await
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn remove_std(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    tokio::task::block_in_place(|| {
        let meta = std::fs::metadata(path)?;
        if meta.is_dir() {
            std::fs::remove_dir(path)
        } else {
            std::fs::remove_file(path)
        }
    })
}

pub fn remove_all_std(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    tokio::task::block_in_place(|| {
        let meta = std::fs::metadata(path)?;
        if meta.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        }
    })
}

pub async fn mkdir(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir(path.as_ref()).await
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    fs::rename(from, to).await
}

pub fn rename_std(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    tokio::task::block_in_place(|| std::fs::rename(from, to))
}

#[tracing::instrument(level = "debug", skip_all)]
// io_uring-accelerated whole-file read on Linux when feature is enabled.
// Fallback to Tokio elsewhere.
#[cfg(all(target_os = "linux", feature = "uring-io"))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_file(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    use std::path::PathBuf;

    let path: PathBuf = path.as_ref().to_path_buf();
    // Run a dedicated tokio-uring runtime in a blocking thread for this op.
    // This keeps integration simple without changing the outer tokio runtime.
    task::spawn_blocking(move || {
        tokio_uring::start(async move {
            use tokio_uring::buf::IoBufMut;
            use tokio_uring::fs::File as UringFile;

            // Get size via standard metadata (cheap and fine here).
            let meta = std::fs::metadata(&path)?;
            if meta.is_dir() {
                return Err(io::Error::new(io::ErrorKind::Other, "is a directory"));
            }
            let size = meta.len() as usize;

            let file = UringFile::open(&path).await?;

            // Read entire file into a single buffer via read_at(0).
            let buf = vec![0u8; size];
            let (res, mut buf) = file.read_at(buf, 0).await;
            match res {
                Ok(n) => {
                    buf.truncate(n);
                    Ok::<_, io::Error>(buf)
                }
                Err(e) => Err(e),
            }
        })
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uring join error: {e}")))?
}

#[cfg(not(all(target_os = "linux", feature = "uring-io")))]
pub async fn read_file(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    fs::read(path.as_ref()).await
}

// Read a specific range [offset, offset+length) from file.
#[cfg(all(target_os = "linux", feature = "uring-io"))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_file_range(path: impl AsRef<Path>, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    use std::path::PathBuf;
    let path: PathBuf = path.as_ref().to_path_buf();
    task::spawn_blocking(move || {
        tokio_uring::start(async move {
            use tokio_uring::buf::IoBufMut;
            use tokio_uring::fs::File as UringFile;

            let file = UringFile::open(&path).await?;
            let mut buf = vec![0u8; length];
            let (res, mut buf) = file.read_at(buf, offset).await;
            match res {
                Ok(n) => {
                    buf.truncate(n);
                    Ok::<_, io::Error>(buf)
                }
                Err(e) => Err(e),
            }
        })
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uring join error: {e}")))?
}

#[cfg(not(all(target_os = "linux", feature = "uring-io")))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn read_file_range(path: impl AsRef<Path>, offset: u64, length: usize) -> io::Result<Vec<u8>> {
    let mut f = File::open(path).await?;
    if offset > 0 {
        f.seek(std::io::SeekFrom::Start(offset)).await?;
    }
    let mut buf = vec![0u8; length];
    let n = f.read(&mut buf).await?;
    buf.truncate(n);
    Ok(buf)
}

// Write whole buffer to a file (truncate) with io_uring acceleration when enabled.
#[cfg(all(target_os = "linux", feature = "uring-io"))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn write_file(path: impl AsRef<Path>, data: &[u8], truncate: bool) -> io::Result<()> {
    use std::path::PathBuf;
    let path: PathBuf = path.as_ref().to_path_buf();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || {
        tokio_uring::start(async move {
            use tokio_uring::buf::IoBuf;
            use tokio_uring::fs::OpenOptions as UringOpenOptions;

            let mut opts = UringOpenOptions::new();
            opts.write(true).create(true);
            if truncate {
                opts.truncate(true);
            }
            let file = opts.open(&path).await?;

            let mut written: usize = 0;
            let total = data.len();
            while written < total {
                let slice = data[written..].to_vec();
                let (res, _buf) = file.write_at(slice, written as u64).await;
                match res {
                    Ok(0) => break,
                    Ok(n) => written += n,
                    Err(e) => return Err(e),
                }
            }
            Ok::<_, io::Error>(())
        })
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uring join error: {e}")))??;
    Ok(())
}

#[cfg(not(all(target_os = "linux", feature = "uring-io")))]
pub async fn write_file(path: impl AsRef<Path>, data: &[u8], _truncate: bool) -> io::Result<()> {
    tokio::fs::write(path, data).await
}

// Append buffer to a file using io_uring when enabled.
#[cfg(all(target_os = "linux", feature = "uring-io"))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn append_file(path: impl AsRef<Path>, data: &[u8]) -> io::Result<()> {
    use std::path::PathBuf;
    let path: PathBuf = path.as_ref().to_path_buf();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || {
        tokio_uring::start(async move {
            use tokio_uring::fs::OpenOptions as UringOpenOptions;

            let mut opts = UringOpenOptions::new();
            opts.write(true).create(true);
            let file = opts.open(&path).await?;

            // Determine current file size to append at end
            let meta = std::fs::metadata(&path)?;
            let mut offset = meta.len();

            let mut written = 0usize;
            while written < data.len() {
                let slice = data[written..].to_vec();
                let (res, _buf) = file.write_at(slice, offset).await;
                match res {
                    Ok(0) => break,
                    Ok(n) => {
                        written += n;
                        offset += n as u64;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok::<_, io::Error>(())
        })
    })
    .await
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("uring join error: {e}")))??;
    Ok(())
}

#[cfg(not(all(target_os = "linux", feature = "uring-io")))]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn append_file(path: impl AsRef<Path>, data: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut f = File::options().create(true).append(true).open(path).await?;
    f.write_all(data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_file_mode_constants() {
        assert_eq!(O_RDONLY, 0x00000);
        assert_eq!(O_WRONLY, 0x00001);
        assert_eq!(O_RDWR, 0x00002);
        assert_eq!(O_CREATE, 0x00040);
        assert_eq!(O_TRUNC, 0x00200);
        assert_eq!(O_APPEND, 0x00400);
    }

    #[tokio::test]
    async fn test_open_file_read_only() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_readonly.txt");

        // Create a test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Test opening in read-only mode
        let file = open_file(&file_path, O_RDONLY).await;
        assert!(file.is_ok());
    }

    #[tokio::test]
    async fn test_open_file_write_only() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_writeonly.txt");

        // Test opening in write-only mode with create flag
        let mut file = open_file(&file_path, O_WRONLY | O_CREATE).await.unwrap();

        // Should be able to write
        file.write_all(b"write test").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_file_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_readwrite.txt");

        // Test opening in read-write mode with create flag
        let mut file = open_file(&file_path, O_RDWR | O_CREATE).await.unwrap();

        // Should be able to write and read
        file.write_all(b"read-write test").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_file_append() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_append.txt");

        // Create initial content
        tokio::fs::write(&file_path, b"initial").await.unwrap();

        // Open in append mode
        let mut file = open_file(&file_path, O_WRONLY | O_APPEND).await.unwrap();
        file.write_all(b" appended").await.unwrap();
        file.flush().await.unwrap();

        // Verify content
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "initial appended");
    }

    #[tokio::test]
    async fn test_open_file_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_truncate.txt");

        // Create initial content
        tokio::fs::write(&file_path, b"initial content").await.unwrap();

        // Open with truncate flag
        let mut file = open_file(&file_path, O_WRONLY | O_TRUNC).await.unwrap();
        file.write_all(b"new").await.unwrap();
        file.flush().await.unwrap();

        // Verify content was truncated
        let content = tokio::fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(content, "new");
    }

    #[tokio::test]
    async fn test_access() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_access.txt");

        // Should fail for non-existent file
        assert!(access(&file_path).await.is_err());

        // Create file and test again
        tokio::fs::write(&file_path, b"test").await.unwrap();
        assert!(access(&file_path).await.is_ok());
    }

    #[test]
    fn test_access_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_access_std.txt");

        // Should fail for non-existent file
        assert!(access_std(&file_path).is_err());

        // Create file and test again
        std::fs::write(&file_path, b"test").unwrap();
        assert!(access_std(&file_path).is_ok());
    }

    #[tokio::test]
    async fn test_lstat() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_lstat.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Test lstat
        let metadata = lstat(&file_path).await.unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 12); // "test content" is 12 bytes
    }

    #[test]
    fn test_lstat_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_lstat_std.txt");

        // Create test file
        std::fs::write(&file_path, b"test content").unwrap();

        // Test lstat_std
        let metadata = lstat_std(&file_path).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.len(), 12); // "test content" is 12 bytes
    }

    #[tokio::test]
    async fn test_make_dir_all() {
        let temp_dir = TempDir::new().unwrap();
        let nested_path = temp_dir.path().join("level1").join("level2").join("level3");

        // Should create nested directories
        assert!(make_dir_all(&nested_path).await.is_ok());
        assert!(nested_path.exists());
        assert!(nested_path.is_dir());
    }

    #[tokio::test]
    async fn test_remove_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_remove.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test").await.unwrap();
        assert!(file_path.exists());

        // Remove file
        assert!(remove(&file_path).await.is_ok());
        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn test_remove_directory() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_dir");

        // Create test directory
        tokio::fs::create_dir(&dir_path).await.unwrap();
        assert!(dir_path.exists());

        // Remove directory
        assert!(remove(&dir_path).await.is_ok());
        assert!(!dir_path.exists());
    }

    #[tokio::test]
    async fn test_remove_all() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_all");
        let file_path = dir_path.join("nested_file.txt");

        // Create nested structure
        tokio::fs::create_dir(&dir_path).await.unwrap();
        tokio::fs::write(&file_path, b"nested content").await.unwrap();

        // Remove all
        assert!(remove_all(&dir_path).await.is_ok());
        assert!(!dir_path.exists());
    }

    #[test]
    fn test_remove_std() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_remove_std.txt");

        // Create test file
        std::fs::write(&file_path, b"test").unwrap();
        assert!(file_path.exists());

        // Remove file
        assert!(remove_std(&file_path).is_ok());
        assert!(!file_path.exists());
    }

    #[test]
    fn test_remove_all_std() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_remove_all_std");
        let file_path = dir_path.join("nested_file.txt");

        // Create nested structure
        std::fs::create_dir(&dir_path).unwrap();
        std::fs::write(&file_path, b"nested content").unwrap();

        // Remove all
        assert!(remove_all_std(&dir_path).is_ok());
        assert!(!dir_path.exists());
    }

    #[tokio::test]
    async fn test_mkdir() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_mkdir");

        // Create directory
        assert!(mkdir(&dir_path).await.is_ok());
        assert!(dir_path.exists());
        assert!(dir_path.is_dir());
    }

    #[tokio::test]
    async fn test_rename() {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old_name.txt");
        let new_path = temp_dir.path().join("new_name.txt");

        // Create test file
        tokio::fs::write(&old_path, b"test content").await.unwrap();
        assert!(old_path.exists());
        assert!(!new_path.exists());

        // Rename file
        assert!(rename(&old_path, &new_path).await.is_ok());
        assert!(!old_path.exists());
        assert!(new_path.exists());

        // Verify content preserved
        let content = tokio::fs::read_to_string(&new_path).await.unwrap();
        assert_eq!(content, "test content");
    }

    #[test]
    fn test_rename_std() {
        let temp_dir = TempDir::new().unwrap();
        let old_path = temp_dir.path().join("old_name_std.txt");
        let new_path = temp_dir.path().join("new_name_std.txt");

        // Create test file
        std::fs::write(&old_path, b"test content").unwrap();
        assert!(old_path.exists());
        assert!(!new_path.exists());

        // Rename file
        assert!(rename_std(&old_path, &new_path).is_ok());
        assert!(!old_path.exists());
        assert!(new_path.exists());

        // Verify content preserved
        let content = std::fs::read_to_string(&new_path).unwrap();
        assert_eq!(content, "test content");
    }

    #[tokio::test]
    async fn test_read_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_read.txt");

        let test_content = b"This is test content for reading";
        tokio::fs::write(&file_path, test_content).await.unwrap();

        // Read file
        let read_content = read_file(&file_path).await.unwrap();
        assert_eq!(read_content, test_content);
    }

    #[tokio::test]
    async fn test_read_file_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("nonexistent.txt");

        // Should fail for non-existent file
        assert!(read_file(&file_path).await.is_err());
    }

    #[tokio::test]
    async fn test_same_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_same.txt");

        // Create test file
        tokio::fs::write(&file_path, b"test content").await.unwrap();

        // Get metadata twice
        let metadata1 = tokio::fs::metadata(&file_path).await.unwrap();
        let metadata2 = tokio::fs::metadata(&file_path).await.unwrap();

        // Should be the same file
        assert!(same_file(&metadata1, &metadata2));
    }

    #[tokio::test]
    async fn test_different_files() {
        let temp_dir = TempDir::new().unwrap();
        let file1_path = temp_dir.path().join("file1.txt");
        let file2_path = temp_dir.path().join("file2.txt");

        // Create two different files
        tokio::fs::write(&file1_path, b"content1").await.unwrap();
        tokio::fs::write(&file2_path, b"content2").await.unwrap();

        // Get metadata
        let metadata1 = tokio::fs::metadata(&file1_path).await.unwrap();
        let metadata2 = tokio::fs::metadata(&file2_path).await.unwrap();

        // Should be different files
        assert!(!same_file(&metadata1, &metadata2));
    }
}
