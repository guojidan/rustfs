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

//! 分片上传加密功能的分步测试用例
//!
//! 这个测试套件将验证分片上传加密功能的每一个步骤：
//! 1. 测试基础的单分片加密（验证加密基础逻辑）
//! 2. 测试多分片上传（验证分片拼接逻辑）
//! 3. 测试加密元数据的保存和读取
//! 4. 测试完整的分片上传加密流程

use super::common::LocalKMSTestEnvironment;
use crate::common::{TEST_BUCKET, init_logging};
use serial_test::serial;
use tracing::{debug, info};

/// 步骤1：测试基础单文件加密功能（确保SSE-S3在非分片场景下正常工作）
#[tokio::test]
#[serial]
async fn test_step1_basic_single_file_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 步骤1：测试基础单文件加密功能");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    // 测试小文件加密（应该会内联存储）
    let test_data = b"Hello, this is a small test file for SSE-S3!";
    let object_key = "test-single-file-encrypted";

    info!("📤 上传小文件（{}字节），启用SSE-S3加密", test_data.len());
    let put_response = s3_client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    debug!("PUT响应ETag: {:?}", put_response.e_tag());
    debug!("PUT响应SSE: {:?}", put_response.server_side_encryption());

    // 验证PUT响应包含正确的加密头
    assert_eq!(
        put_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    info!("📥 下载文件并验证加密状态");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET响应SSE: {:?}", get_response.server_side_encryption());

    // 验证GET响应包含正确的加密头
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // 验证数据完整性
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(&downloaded_data[..], test_data);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 步骤1通过：基础单文件加密功能正常");
    Ok(())
}

/// 步骤2：测试不加密的分片上传（确保分片上传基础功能正常）
#[tokio::test]
#[serial]
async fn test_step2_basic_multipart_upload_without_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 步骤2：测试不加密的分片上传");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-no-encryption";
    let part_size = 5 * 1024 * 1024; // 5MB per part (S3 minimum)
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // 生成测试数据（有明显的模式便于验证）
    let test_data: Vec<u8> = (0..total_size).map(|i| (i % 256) as u8).collect();

    info!("🚀 开始分片上传（无加密）：{} parts，每个 {}MB", total_parts, part_size / (1024 * 1024));

    // 步骤1：创建分片上传
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 创建分片上传，ID: {}", upload_id);

    // 步骤2：上传各个分片
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("📤 上传分片 {} ({} bytes)", part_number, part_data.len());

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("分片 {} 上传完成，ETag: {}", part_number, etag);
    }

    // 步骤3：完成分片上传
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 完成分片上传");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("完成分片上传，ETag: {:?}", complete_output.e_tag());

    // 步骤4：下载并验证
    info!("📥 下载文件并验证数据完整性");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 步骤2通过：不加密的分片上传功能正常");
    Ok(())
}

/// 步骤3：测试分片上传 + SSE-S3加密（重点测试）
#[tokio::test]
#[serial]
async fn test_step3_multipart_upload_with_sse_s3() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 步骤3：测试分片上传 + SSE-S3加密");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-multipart-sse-s3";
    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // 生成测试数据
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i / 1000) % 256) as u8).collect();

    info!(
        "🔐 开始分片上传（SSE-S3加密）：{} parts，每个 {}MB",
        total_parts,
        part_size / (1024 * 1024)
    );

    // 步骤1：创建分片上传并启用SSE-S3
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 创建加密分片上传，ID: {}", upload_id);

    // 验证CreateMultipartUpload响应（如果有SSE头的话）
    if let Some(sse) = create_multipart_output.server_side_encryption() {
        debug!("CreateMultipartUpload包含SSE响应: {:?}", sse);
        assert_eq!(sse, &aws_sdk_s3::types::ServerSideEncryption::Aes256);
    } else {
        debug!("CreateMultipartUpload不包含SSE响应头（某些实现中正常）");
    }

    // 步骤2：上传各个分片
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!("🔐 上传加密分片 {} ({} bytes)", part_number, part_data.len());

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("加密分片 {} 上传完成，ETag: {}", part_number, etag);
    }

    // 步骤3：完成分片上传
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 完成加密分片上传");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("完成加密分片上传，ETag: {:?}", complete_output.e_tag());

    // 步骤4：HEAD请求检查元数据
    info!("📋 检查对象元数据");
    let head_response = s3_client.head_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("HEAD响应 SSE: {:?}", head_response.server_side_encryption());
    debug!("HEAD响应 元数据: {:?}", head_response.metadata());

    // 步骤5：GET请求下载并验证
    info!("📥 下载加密文件并验证");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    debug!("GET响应 SSE: {:?}", get_response.server_side_encryption());

    // 🎯 关键验证：GET响应必须包含SSE-S3加密头
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // 验证数据完整性
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 步骤3通过：分片上传 + SSE-S3加密功能正常");
    Ok(())
}

/// 步骤4：测试更大的分片上传（测试流式加密）
#[tokio::test]
#[serial]
async fn test_step4_large_multipart_upload_with_encryption() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 步骤4：测试大文件分片上传加密");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let object_key = "test-large-multipart-encrypted";
    let part_size = 6 * 1024 * 1024; // 6MB per part (大于1MB加密块大小)
    let total_parts = 3; // 总共18MB
    let total_size = part_size * total_parts;

    info!(
        "🗂️ 生成大文件测试数据：{} parts，每个 {}MB，总计 {}MB",
        total_parts,
        part_size / (1024 * 1024),
        total_size / (1024 * 1024)
    );

    // 生成大文件测试数据（使用复杂模式便于验证）
    let test_data: Vec<u8> = (0..total_size)
        .map(|i| {
            let part_num = i / part_size;
            let offset_in_part = i % part_size;
            ((part_num * 100 + offset_in_part / 1000) % 256) as u8
        })
        .collect();

    info!("🔐 开始大文件分片上传（SSE-S3加密）");

    // 创建分片上传
    let create_multipart_output = s3_client
        .create_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .send()
        .await?;

    let upload_id = create_multipart_output.upload_id().unwrap();
    info!("📋 创建大文件加密分片上传，ID: {}", upload_id);

    // 上传各个分片
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        info!(
            "🔐 上传大文件加密分片 {} ({:.2}MB)",
            part_number,
            part_data.len() as f64 / (1024.0 * 1024.0)
        );

        let upload_part_output = s3_client
            .upload_part()
            .bucket(TEST_BUCKET)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()))
            .send()
            .await?;

        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("大文件加密分片 {} 上传完成，ETag: {}", part_number, etag);
    }

    // 完成分片上传
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    info!("🔗 完成大文件加密分片上传");
    let complete_output = s3_client
        .complete_multipart_upload()
        .bucket(TEST_BUCKET)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    debug!("完成大文件加密分片上传，ETag: {:?}", complete_output.e_tag());

    // 下载并验证
    info!("📥 下载大文件并验证");
    let get_response = s3_client.get_object().bucket(TEST_BUCKET).key(object_key).send().await?;

    // 验证加密头
    assert_eq!(
        get_response.server_side_encryption(),
        Some(&aws_sdk_s3::types::ServerSideEncryption::Aes256)
    );

    // 验证数据完整性
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);

    // 逐字节验证数据（对于大文件更严格）
    for (i, (&actual, &expected)) in downloaded_data.iter().zip(test_data.iter()).enumerate() {
        if actual != expected {
            panic!("大文件数据在第{}字节不匹配: 实际={}, 期待={}", i, actual, expected);
        }
    }

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 步骤4通过：大文件分片上传加密功能正常");
    Ok(())
}

/// 步骤5：测试所有加密类型的分片上传
#[tokio::test]
#[serial]
async fn test_step5_all_encryption_types_multipart() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();
    info!("🧪 步骤5：测试所有加密类型的分片上传");

    let mut kms_env = LocalKMSTestEnvironment::new().await?;
    let _default_key_id = kms_env.start_rustfs_for_local_kms().await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let s3_client = kms_env.base_env.create_s3_client();
    kms_env.base_env.create_test_bucket(TEST_BUCKET).await?;

    let part_size = 5 * 1024 * 1024; // 5MB per part
    let total_parts = 2;
    let total_size = part_size * total_parts;

    // 测试SSE-KMS
    info!("🔐 测试 SSE-KMS 分片上传");
    test_multipart_encryption_type(
        &s3_client,
        TEST_BUCKET,
        "test-multipart-sse-kms",
        total_size,
        part_size,
        total_parts,
        EncryptionType::SSEKMS,
    )
    .await?;

    // 测试SSE-C
    info!("🔐 测试 SSE-C 分片上传");
    test_multipart_encryption_type(
        &s3_client,
        TEST_BUCKET,
        "test-multipart-sse-c",
        total_size,
        part_size,
        total_parts,
        EncryptionType::SSEC,
    )
    .await?;

    kms_env.base_env.delete_test_bucket(TEST_BUCKET).await?;
    info!("✅ 步骤5通过：所有加密类型的分片上传功能正常");
    Ok(())
}

#[derive(Debug)]
enum EncryptionType {
    SSEKMS,
    SSEC,
}

/// 辅助函数：测试特定加密类型的分片上传
async fn test_multipart_encryption_type(
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    object_key: &str,
    total_size: usize,
    part_size: usize,
    total_parts: usize,
    encryption_type: EncryptionType,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 生成测试数据
    let test_data: Vec<u8> = (0..total_size).map(|i| ((i * 7) % 256) as u8).collect();

    // 准备SSE-C所需的密钥（如果需要）
    let (sse_c_key, sse_c_md5) = if matches!(encryption_type, EncryptionType::SSEC) {
        let key = "01234567890123456789012345678901";
        let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
        let key_md5 = format!("{:x}", md5::compute(key));
        (Some(key_b64), Some(key_md5))
    } else {
        (None, None)
    };

    info!("📋 创建分片上传 - {:?}", encryption_type);

    // 创建分片上传
    let mut create_request = s3_client.create_multipart_upload().bucket(bucket).key(object_key);

    create_request = match encryption_type {
        EncryptionType::SSEKMS => create_request.server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms),
        EncryptionType::SSEC => create_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_md5.as_ref().unwrap()),
    };

    let create_multipart_output = create_request.send().await?;
    let upload_id = create_multipart_output.upload_id().unwrap();

    // 上传分片
    let mut completed_parts = Vec::new();
    for part_number in 1..=total_parts {
        let start = (part_number - 1) * part_size;
        let end = std::cmp::min(start + part_size, total_size);
        let part_data = &test_data[start..end];

        let mut upload_request = s3_client
            .upload_part()
            .bucket(bucket)
            .key(object_key)
            .upload_id(upload_id)
            .part_number(part_number as i32)
            .body(aws_sdk_s3::primitives::ByteStream::from(part_data.to_vec()));

        // SSE-C需要在每个UploadPart请求中包含密钥
        if matches!(encryption_type, EncryptionType::SSEC) {
            upload_request = upload_request
                .sse_customer_algorithm("AES256")
                .sse_customer_key(sse_c_key.as_ref().unwrap())
                .sse_customer_key_md5(sse_c_md5.as_ref().unwrap());
        }

        let upload_part_output = upload_request.send().await?;
        let etag = upload_part_output.e_tag().unwrap().to_string();
        completed_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(part_number as i32)
                .e_tag(&etag)
                .build(),
        );

        debug!("{:?} 分片 {} 上传完成", encryption_type, part_number);
    }

    // 完成分片上传
    let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    let _complete_output = s3_client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(object_key)
        .upload_id(upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await?;

    // 下载并验证
    let mut get_request = s3_client.get_object().bucket(bucket).key(object_key);

    // SSE-C需要在GET请求中包含密钥
    if matches!(encryption_type, EncryptionType::SSEC) {
        get_request = get_request
            .sse_customer_algorithm("AES256")
            .sse_customer_key(sse_c_key.as_ref().unwrap())
            .sse_customer_key_md5(sse_c_md5.as_ref().unwrap());
    }

    let get_response = get_request.send().await?;

    // 验证加密头
    match encryption_type {
        EncryptionType::SSEKMS => {
            assert_eq!(
                get_response.server_side_encryption(),
                Some(&aws_sdk_s3::types::ServerSideEncryption::AwsKms)
            );
        }
        EncryptionType::SSEC => {
            assert_eq!(get_response.sse_customer_algorithm(), Some("AES256"));
        }
    }

    // 验证数据完整性
    let downloaded_data = get_response.body.collect().await?.into_bytes();
    assert_eq!(downloaded_data.len(), total_size);
    assert_eq!(&downloaded_data[..], &test_data[..]);

    info!("✅ {:?} 分片上传测试通过", encryption_type);
    Ok(())
}
