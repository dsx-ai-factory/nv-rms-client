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
        delay: Duration,
        status_response: rm::GetScaleUpFabricStatusResponse,
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
    async fn client_requires_observation_only_for_inspection_including_failed_responses() {
        for (inspection_requested, observation_present, status) in [
            (false, false, rm::ReturnCode::Success),
            (false, false, rm::ReturnCode::Failure),
            (false, true, rm::ReturnCode::Success),
            (false, true, rm::ReturnCode::Failure),
            (true, false, rm::ReturnCode::Success),
            (true, false, rm::ReturnCode::Failure),
            (true, true, rm::ReturnCode::Success),
            (true, true, rm::ReturnCode::Failure),
        ] {
            let response = rm::GetScaleUpFabricStatusResponse {
                status: status.into(),
                fabric_status: (!observation_present).then(rm::ScaleUpFabricStatus::default),
                observation: observation_present.then(rm::ScaleUpFabricObservation::default),
                ..Default::default()
            };

            let fixture = Fixture::new(State {
                status_response: response.clone(),
                ..Default::default()
            })
            .await;

            let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

            let request = rm::GetScaleUpFabricStatusRequest {
                inspection: inspection_requested.then(rm::ScaleUpFabricInspection::default),
                ..Default::default()
            };

            let result = tokio::time::timeout(
                Duration::from_secs(5),
                client.get_scale_up_fabric_status(request),
            )
            .await
            .expect("status RPC should finish before the timeout");

            if inspection_requested && !observation_present {
                let Err(RackManagerError::ApiInvocationError(error)) = result else {
                    panic!("inspection response without observation must be unsupported");
                };

                assert_eq!(error.code(), tonic::Code::Unimplemented);

                assert!(
                    error
                        .message()
                        .contains("omitted the requested live observation")
                );
            } else {
                assert_eq!(result.unwrap(), response);
            }
        }
    }

    #[tokio::test]
    async fn configuration_requests_forward_inventory_layouts_and_return_assignments() {
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

        let full_assignment = v2::ScaleUpFabricAssignment {
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
                    v2::ScaleUpFabricAssignment {
                        compute_node_ids: vec!["compute-3".to_owned(), "compute-4".to_owned()],
                        switch_node_ids: vec!["switch-2".to_owned()],
                        primary_switch_node_id: "switch-2".to_owned(),
                    },
                    v2::ScaleUpFabricAssignment {
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
                vec![v2::ScaleUpFabricAssignment {
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
                resolved_fabrics: assignments,
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
