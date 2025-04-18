use crate::dispatcher::{
    connect_v3, connect_v5, control_factory_v3, control_factory_v5, publish_factory_v3,
    publish_factory_v5,
};
use crate::error::ServerError;
use crate::middleware::RequestLogger;
use ntex::tls::rustls::{PeerCert, TlsAcceptor, TlsServerFilter};
use ntex::util::Ready;
use ntex::{chain_factory, fn_service};
use ntex_io::{Io, Layer};
use ntex_mqtt::{v3, v5, MqttError, MqttServer};
use pingora_load_balancing::selection::Consistent;
use pingora_load_balancing::LoadBalancer;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, LazyLock};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;
use crate::upstream::create_lb;

mod dispatcher;
mod error;
mod handler;
mod middleware;
mod session;
mod upstream;

static UPSTREAM: LazyLock<Arc<LoadBalancer<Consistent>>> = LazyLock::new(create_lb);

async fn listen_tcp() -> std::io::Result<()> {
    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1884", move |_| {
            let mqtt_v3_server = v3::MqttServer::new(connect_v3)
                .control(control_factory_v3())
                .publish(publish_factory_v3())
                .middleware(RequestLogger)
                // .middleware(fn_pub_ack_factory_v3())
                // .middleware(fn_handle_packet_id())
                // .middleware(fn_auth)
                .finish();

            let mqtt_v5_server = v5::MqttServer::new(connect_v5)
                .control(control_factory_v5())
                .publish(publish_factory_v5())
                .finish();

            MqttServer::new().v3(mqtt_v3_server).v5(mqtt_v5_server)
        })?
        .workers(1)
        .run()
        .await
}

async fn listen_tls() -> std::io::Result<()> {
    let cert_file = &mut BufReader::new(File::open("resources/server.chain.crt")?);
    let key_file = &mut BufReader::new(File::open("resources/server.pkcs8.key")?);
    let keys = rustls_pemfile::private_key(key_file)?.unwrap();

    let cert_chain = rustls_pemfile::certs(cert_file).collect::<Result<Vec<_>, _>>()?;
    let mut root_store = RootCertStore::empty();
    root_store.add_parsable_certificates(cert_chain.clone());
    let client_verifier = WebPkiClientVerifier::builder(root_store.into())
        .build()
        .unwrap();

    let tls_config = Arc::new(
        ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(cert_chain, keys)
            .unwrap(),
    );

    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1885", move |_| {
            chain_factory(TlsAcceptor::new(tls_config.clone()))
                .map_err(|err| {
                    println!("Tls handshake failed: {}", err);
                    MqttError::Service(ServerError)
                })
                .and_then(fn_service(|io: Io<Layer<TlsServerFilter>>| {
                    // Handle peer certificate
                    println!(
                        "Incoming tls connection: peer certificate: {:?}",
                        io.query::<PeerCert>().as_ref().map(|cert: &PeerCert| {
                            X509Certificate::from_der(&cert.0)
                                .unwrap()
                                .1
                                .subject()
                                .to_string()
                        })
                    );
                    Ready::Ok(io)
                }))
                .and_then({
                    let mqtt_v3_server = v3::MqttServer::new(connect_v3)
                        .control(control_factory_v3())
                        .publish(publish_factory_v3())
                        .middleware(RequestLogger)
                        // .middleware(fn_pub_ack_factory_v3())
                        // .middleware(fn_handle_packet_id())
                        // .middleware(fn_auth)
                        .finish();

                    let mqtt_v5_server = v5::MqttServer::new(connect_v5)
                        .control(control_factory_v5())
                        .publish(publish_factory_v5())
                        .finish();

                    MqttServer::new().v3(mqtt_v3_server).v5(mqtt_v5_server)
                })
            // TODO: add error handler
            // .then(service_error_handler)
        })?
        .workers(1)
        .run()
        .await
}

#[ntex::main]
async fn main() {
    let _ = UPSTREAM.clone();
    
    let tcp_handle = ntex::rt::spawn(listen_tcp());
    let tls_handle = ntex::rt::spawn(listen_tls());
    let _ = tokio::join!(tcp_handle, tls_handle);
}
