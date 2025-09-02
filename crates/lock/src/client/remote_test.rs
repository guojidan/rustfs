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

#[cfg(test)]
mod tests {
    use super::super::remote::RemoteClient;
    use crate::types::{LockRequest, LockType};
    use std::time::Duration;

    #[tokio::test]
    async fn test_remote_client_creation() {
        let client = RemoteClient::new("http://localhost:8080".to_string());
        assert!(!client.is_local().await);
    }

    #[tokio::test]
    async fn test_remote_client_from_url() {
        let url = url::Url::parse("http://localhost:8080").unwrap();
        let client = RemoteClient::from_url(url);
        assert!(!client.is_local().await);
    }

    #[tokio::test]
    async fn test_remote_client_cache_functionality() {
        let client = RemoteClient::new("http://localhost:8080".to_string());
        let resource_name = format!("test-cache-{}", uuid::Uuid::new_v4());
        
        // Create a mock request
        let request = LockRequest::new(&resource_name, LockType::Exclusive, "test-owner")
            .with_acquire_timeout(Duration::from_millis(10));
            
        // Test that cache exists
        assert!(client.cache.is_empty());
    }

    #[tokio::test]
    async fn test_remote_client_connection_pool() {
        let client = RemoteClient::new("http://localhost:8080".to_string());
        // Test that client pool exists
        assert_eq!(client.client_pool.max_size, 10);
    }
}