/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#[cfg(feature = "client")]
mod client_tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use librms::client::RmsApiConfig;
    use librms::client_config::RmsClientConfig;
    use librms::protos::rack_manager as rm;
    use librms::protos::rack_manager_v2 as v2;
    use librms::{RackManagerApi, RackManagerError, RmsApi};

    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::net::TcpListener;
    use tokio::task::{JoinHandle, JoinSet};

    type RpcFuture<T> =
        Pin<Box<dyn Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send>>;

    #[derive(Default)]
    struct State {
        submissions: Vec<v2::ConfigureScaleUpFabricManagerRequest>,
        inspections: Vec<rm::InspectScaleUpFabricsRequest>,
        delay: Duration,
        status_response: rm::GetScaleUpFabricStatusResponse,
        inspection_response: rm::InspectScaleUpFabricsResponse,
        inspection_error: Option<tonic::Status>,
        configure_response: v2::ConfigureScaleUpFabricManagerResponse,
    }

    struct VersionRpc;

    impl tonic::server::UnaryService<rm::GetVersionRequest> for VersionRpc {
        type Response = rm::GetVersionResponse;
        type Future = RpcFuture<Self::Response>;

        fn call(&mut self, _: tonic::Request<rm::GetVersionRequest>) -> Self::Future {
            Box::pin(async {
                Ok(tonic::Response::new(rm::GetVersionResponse {
                    version: "fixture".to_owned(),
                }))
            })
        }
    }

    struct ConfigureRpc(Arc<Mutex<State>>);

    impl tonic::server::UnaryService<v2::ConfigureScaleUpFabricManagerRequest> for ConfigureRpc {
        type Response = v2::ConfigureScaleUpFabricManagerResponse;
        type Future = RpcFuture<Self::Response>;

        fn call(
            &mut self,
            request: tonic::Request<v2::ConfigureScaleUpFabricManagerRequest>,
        ) -> Self::Future {
            let (delay, response) = {
                let mut state = self.0.lock().unwrap();

                state.submissions.push(request.into_inner());

                (state.delay, state.configure_response.clone())
            };

            Box::pin(async move {
                tokio::time::sleep(delay).await;

                Ok(tonic::Response::new(response))
            })
        }
    }

    struct StatusRpc(Arc<Mutex<State>>);

    impl tonic::server::UnaryService<rm::GetScaleUpFabricStatusRequest> for StatusRpc {
        type Response = rm::GetScaleUpFabricStatusResponse;
        type Future = RpcFuture<Self::Response>;

        fn call(&mut self, _: tonic::Request<rm::GetScaleUpFabricStatusRequest>) -> Self::Future {
            let (delay, response) = {
                let state = self.0.lock().unwrap();

                (state.delay, state.status_response.clone())
            };

            Box::pin(async move {
                tokio::time::sleep(delay).await;

                Ok(tonic::Response::new(response))
            })
        }
    }

    struct InspectionRpc(Arc<Mutex<State>>);

    impl tonic::server::UnaryService<rm::InspectScaleUpFabricsRequest> for InspectionRpc {
        type Response = rm::InspectScaleUpFabricsResponse;
        type Future = RpcFuture<Self::Response>;

        fn call(
            &mut self,
            request: tonic::Request<rm::InspectScaleUpFabricsRequest>,
        ) -> Self::Future {
            let (delay, response, error) = {
                let mut state = self.0.lock().unwrap();

                state.inspections.push(request.into_inner());

                (
                    state.delay,
                    state.inspection_response.clone(),
                    state.inspection_error.clone(),
                )
            };

            Box::pin(async move {
                tokio::time::sleep(delay).await;

                if let Some(error) = error {
                    return Err(error);
                }

                Ok(tonic::Response::new(response))
            })
        }
    }

    struct Fixture {
        url: String,
        state: Arc<Mutex<State>>,
        server: JoinHandle<()>,
    }

    impl Fixture {
        async fn new(state: State) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(Mutex::new(state));
            let server_state = Arc::clone(&state);

            let server = tokio::spawn(async move {
                let mut connections = JoinSet::new();

                loop {
                    tokio::select! {
                        socket = listener.accept() => {
                            let (socket, _) = socket.unwrap();
                            let state = Arc::clone(&server_state);

                            connections.spawn(async move {
                                let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                                    let state = Arc::clone(&state);

                                    async move {
                                        let response = match request.uri().path() {
                                            "/rack_manager.RackManager/GetVersion" => {
                                                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                                                    .unary(VersionRpc, request).await
                                            }
                                            "/rack_manager_v2.RackManagerV2/ConfigureScaleUpFabricManager" => {
                                                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                                                    .unary(ConfigureRpc(state), request).await
                                            }
                                            "/rack_manager.RackManager/GetScaleUpFabricStatus" => {
                                                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                                                    .unary(StatusRpc(state), request).await
                                            }
                                            "/rack_manager.RackManager/InspectScaleUpFabrics" => {
                                                tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                                                    .unary(InspectionRpc(state), request).await
                                            }
                                            path => panic!("unexpected RPC: {path}"),
                                        };

                                        Ok::<_, Infallible>(response)
                                    }
                                });

                                // A timed-out client may close the HTTP/2 connection.
                                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                                    .serve_connection(TokioIo::new(socket), service).await;
                            });
                        }
                        Some(result) = connections.join_next() => result.unwrap(),
                    }
                }
            });

            Self { url, state, server }
        }

        fn client(&self, config: &RmsClientConfig) -> RackManagerApi {
            RackManagerApi::new(&RmsApiConfig::new(&self.url, config))
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Dropping the server's JoinSet aborts every accepted connection.
            self.server.abort();
        }
    }

    #[tokio::test]
    async fn stored_configuration_responses_pass_through_without_inspection() {
        for status in [rm::ReturnCode::Success, rm::ReturnCode::Failure] {
            let response = rm::GetScaleUpFabricStatusResponse {
                status: status.into(),
                fabric_status: (status == rm::ReturnCode::Success).then(|| {
                    rm::ScaleUpFabricStatus {
                        topology_type: "fixture-topology".into(),
                        ..Default::default()
                    }
                }),
                ..Default::default()
            };

            let fixture = Fixture::new(State {
                status_response: response.clone(),
                ..Default::default()
            })
            .await;

            let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.get_scale_up_fabric_status(rm::GetScaleUpFabricStatusRequest::default()),
            )
            .await
            .expect("status RPC should finish before the timeout");

            assert_eq!(result.unwrap(), response);
        }
    }

    #[tokio::test]
    async fn inspection_forwards_inventory_and_retains_partial_results() {
        for (with_computes, status) in [
            (false, rm::ReturnCode::Success),
            (true, rm::ReturnCode::Success),
            (true, rm::ReturnCode::Failure),
        ] {
            let response = rm::InspectScaleUpFabricsResponse {
                status: status.into(),
                fabrics: vec![rm::ScaleUpFabricInspectionObservation {
                    switch_node_ids: vec!["switch-1".into()],
                    compute_node_ids: vec!["compute-1".into(), "compute-2".into()],
                    primary_switch_node_id: "switch-1".into(),
                    control_plane_state: rm::ScaleUpFabricControlPlaneState::Configured.into(),
                    status: rm::ScaleUpFabricReportedStatus::Abnormal.into(),
                    component_status: Some(rm::ScaleUpFabricComponentStatus {
                        controller: rm::ScaleUpFabricReportedStatus::Normal.into(),
                        compute: rm::ScaleUpFabricReportedStatus::Unknown.into(),
                        switches: rm::ScaleUpFabricReportedStatus::Normal.into(),
                        links: rm::ScaleUpFabricReportedStatus::Abnormal.into(),
                    }),
                    inspection_errors: if status == rm::ReturnCode::Failure {
                        vec![rm::ScaleUpFabricInspectionError {
                            node_id: "switch-1".into(),
                            message: "compute-nodes: resource read failed".into(),
                        }]
                    } else {
                        vec![]
                    },
                }],
                discovery_errors: if status == rm::ReturnCode::Failure {
                    vec![rm::ScaleUpFabricInspectionError {
                        node_id: "switch-2".into(),
                        message: "cluster: resource read failed".into(),
                    }]
                } else {
                    vec![]
                },
                error_message: if status == rm::ReturnCode::Failure {
                    "one or more observations are incomplete or ambiguous".into()
                } else {
                    String::new()
                },
            };

            let fixture = Fixture::new(State {
                inspection_response: response.clone(),
                ..Default::default()
            })
            .await;

            let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

            let request = rm::InspectScaleUpFabricsRequest {
                nodes: Some(rm::NodeSet {
                    nodes: vec![rm::NodeInfo {
                        node_id: "switch-1".into(),
                        ..Default::default()
                    }],
                }),
                domain: Some("fixture.example".into()),
                compute_nodes: with_computes.then(|| rm::NodeSet {
                    nodes: vec![rm::NodeInfo {
                        node_id: "compute-1".into(),
                        ..Default::default()
                    }],
                }),
            };

            let actual = tokio::time::timeout(
                Duration::from_secs(5),
                client.inspect_scale_up_fabrics(request.clone()),
            )
            .await
            .unwrap()
            .unwrap();

            assert_eq!(actual, response);
            assert_eq!(fixture.state.lock().unwrap().inspections, vec![request]);
        }
    }

    #[tokio::test]
    async fn inspection_preserves_unimplemented_from_unsupported_servers() {
        let fixture = Fixture::new(State {
            inspection_error: Some(tonic::Status::unimplemented(
                "inspection RPC is unavailable",
            )),
            ..Default::default()
        })
        .await;

        let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.inspect_scale_up_fabrics(rm::InspectScaleUpFabricsRequest::default()),
        )
        .await
        .unwrap();

        let Err(RackManagerError::ApiInvocationError(error)) = result else {
            panic!("unsupported inspection must return an invocation error");
        };

        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert_eq!(error.message(), "inspection RPC is unavailable");
    }

    #[tokio::test]
    async fn configuration_requests_forward_inventory_layouts_and_return_selected_fabrics() {
        let nodes = rm::NodeSet {
            nodes: ["switch-1", "switch-2"]
                .into_iter()
                .map(|node_id| rm::NodeInfo {
                    node_id: node_id.to_owned(),
                    ..Default::default()
                })
                .collect(),
        };

        let compute_nodes = rm::NodeSet {
            nodes: ["compute-1", "compute-2", "compute-3", "compute-4"]
                .into_iter()
                .map(|node_id| rm::NodeInfo {
                    node_id: node_id.to_owned(),
                    ..Default::default()
                })
                .collect(),
        };

        let partial_layout = v2::ScaleUpFabricLayout {
            fabrics: ["switch-2", "switch-1"]
                .into_iter()
                .map(|primary| v2::ScaleUpFabricSpec {
                    compute_tray_count: 2,
                    switch_tray_count: 1,
                    primary_switch_node_id: Some(primary.to_owned()),
                })
                .collect(),
        };

        let full_layout = v2::ScaleUpFabricLayout {
            fabrics: vec![v2::ScaleUpFabricSpec {
                compute_tray_count: 4,
                switch_tray_count: 2,
                primary_switch_node_id: None,
            }],
        };

        let full_assignment = v2::ScaleUpFabricMembers {
            compute_node_ids: compute_nodes
                .nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect(),
            switch_node_ids: vec!["switch-1".to_owned(), "switch-2".to_owned()],
            primary_switch_node_id: "switch-1".to_owned(),
        };

        for (name, layout, with_computes, assignments) in [
            ("legacy", None, false, vec![]),
            (
                "partial",
                Some(partial_layout),
                true,
                vec![
                    v2::ScaleUpFabricMembers {
                        compute_node_ids: vec!["compute-3".to_owned(), "compute-4".to_owned()],
                        switch_node_ids: vec!["switch-2".to_owned()],
                        primary_switch_node_id: "switch-2".to_owned(),
                    },
                    v2::ScaleUpFabricMembers {
                        compute_node_ids: vec!["compute-1".to_owned(), "compute-2".to_owned()],
                        switch_node_ids: vec!["switch-1".to_owned()],
                        primary_switch_node_id: "switch-1".to_owned(),
                    },
                ],
            ),
            (
                "full",
                Some(full_layout.clone()),
                true,
                vec![full_assignment.clone()],
            ),
            (
                "full_without_computes",
                Some(full_layout),
                false,
                vec![v2::ScaleUpFabricMembers {
                    compute_node_ids: vec![],
                    ..full_assignment
                }],
            ),
        ] {
            let command = v2::ConfigureScaleUpFabricManagerRequest {
                nodes: Some(nodes.clone()),
                primary_switch_node_id: layout.is_none().then(|| "switch-2".to_owned()),
                domain: Some("fixture.example".to_owned()),
                config: Some(v2::ScaleUpFabricConfig {
                    topology_type: "fixture-topology".to_owned(),
                    extra_static_configs: vec![rm::ScaleUpFabricStaticConfig {
                        config_file_name: "fabric.conf".to_owned(),
                        key: "fixture-setting".to_owned(),
                        value: "enabled".to_owned(),
                    }],
                }),
                layout,
                compute_nodes: with_computes.then(|| compute_nodes.clone()),
            };

            let expected_response = v2::ConfigureScaleUpFabricManagerResponse {
                job_id: format!("accepted-{name}"),
                selected_fabrics: assignments,
            };

            let fixture = Fixture::new(State {
                configure_response: expected_response.clone(),
                ..Default::default()
            })
            .await;

            let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

            let response = client
                .configure_scale_up_fabric_manager_v2(command.clone())
                .await
                .unwrap();

            assert_eq!(response, expected_response, "{name}");

            assert_eq!(
                fixture.state.lock().unwrap().submissions,
                vec![command],
                "{name}"
            );
        }
    }
}
