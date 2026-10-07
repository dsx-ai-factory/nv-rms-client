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
            let delay = {
                let mut state = self.0.lock().unwrap();

                state.submissions.push(request.into_inner());

                state.delay
            };

            Box::pin(async move {
                tokio::time::sleep(delay).await;

                Ok(tonic::Response::new(
                    v2::ConfigureScaleUpFabricManagerResponse {
                        job_id: "accepted-job".to_owned(),
                        ..Default::default()
                    },
                ))
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

    fn request(layout: bool, reset: bool) -> v2::ConfigureScaleUpFabricManagerRequest {
        v2::ConfigureScaleUpFabricManagerRequest {
            layout: layout.then(v2::ScaleUpFabricLayout::default),
            reset_fabric_config: reset,
            ..Default::default()
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
    async fn configuration_requests_forward_layout_restoration_and_legacy_unchanged() {
        for command in [
            request(true, false),
            request(false, true),
            request(false, false),
        ] {
            let fixture = Fixture::new(State::default()).await;
            let client = fixture.client(&RmsClientConfig::new(None, None, None, false));

            let response = client
                .configure_scale_up_fabric_manager_v2(command.clone())
                .await
                .unwrap();

            assert_eq!(response.job_id, "accepted-job");
            assert_eq!(fixture.state.lock().unwrap().submissions, vec![command]);
        }
    }
}
