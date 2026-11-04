// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "serde")]

use librms::protos::rack_manager::{
    CompareFirmwareObjectRequest, FirmwareManifestComponentComparison,
};
use prost::Message;

#[test]
fn pldm_comparison_has_structured_native_details_and_round_trips() {
    use librms::protos::rack_manager::{CompareFirmwareObjectByPldmRequest, PldmComparisonDetails};
    let request = CompareFirmwareObjectByPldmRequest {
        object_id: "object".into(),
        ..Default::default()
    };
    assert_eq!(
        CompareFirmwareObjectByPldmRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
    let details: PldmComparisonDetails = serde_json::from_value(serde_json::json!({
        "status":"matched", "all_targets_matched":false,
        "components":[{"name":"GPU_0", "status":"unverified", "package_version":"N/A"}]
    }))
    .unwrap();
    assert!(!details.all_targets_matched);
    assert_eq!(details.components[0].status, "unverified");
    assert!(serde_json::to_value(details).unwrap()["components"].is_array());
}

#[test]
fn legacy_json_requests_and_results_keep_deserializing() {
    let request: CompareFirmwareObjectRequest = serde_json::from_value(serde_json::json!({
        "config_json":"{}", "firmware_type":"prod", "nodes":null,
        "component_filters":{}, "node_descriptor_component_filters":[]
    }))
    .unwrap();
    assert!(request.object_id.is_empty());
    assert!(request.inventory_components.is_empty());
    let row: FirmwareManifestComponentComparison = serde_json::from_value(serde_json::json!({
        "component":"BMC", "installed_component":"FW_BMC_0", "installed_version":"1",
        "proposed_version":"1", "status":1, "target":"", "reason":"", "sku_id":""
    }))
    .unwrap();
    assert!(row.inventory_path.is_empty());
    assert!(row.bundle.is_empty());
}

#[test]
fn stored_object_and_instance_selectors_round_trip_on_the_wire() {
    let request = CompareFirmwareObjectRequest {
        object_id: "GB300_1.0".into(),
        inventory_components: vec!["FW_CX8_0".into()],
        ..Default::default()
    };
    assert_eq!(
        CompareFirmwareObjectRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}
