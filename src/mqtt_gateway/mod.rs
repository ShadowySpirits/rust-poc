use self::dispatcher::{
    connect_v3, connect_v5, control_factory_v3, control_factory_v5, publish_factory_v3,
    publish_factory_v5,
};
use self::error::ServerError;
use self::middleware::RequestLogger;
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
use self::upstream::create_lb;
use log::{info, error, debug};
use env_logger;

mod dispatcher;
mod error;
mod handler;
mod middleware;
mod session;
mod upstream;

static UPSTREAM: LazyLock<Arc<LoadBalancer<Consistent>>> = LazyLock::new(create_lb);

async fn listen_tcp() -> std::io::Result<()> {
    info!("Starting MQTT TCP server on 0.0.0.0:1884");
    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1884", move |_| {
            debug!("Initializing MQTT v3 server");
            let mqtt_v3_server = v3::MqttServer::new(connect_v3)
                .control(control_factory_v3())
                .publish(publish_factory_v3())
                .middleware(RequestLogger)
                // .middleware(fn_pub_ack_factory_v3())
                // .middleware(fn_handle_packet_id())
                // .middleware(fn_auth)
                .finish();

            debug!("Initializing MQTT v5 server");
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
    info!("Starting MQTT TLS server on 0.0.0.0:1885");
    let cert_file = &mut BufReader::new(File::open("resources/server.chain.crt")?);
    let key_file = &mut BufReader::new(File::open("resources/server.pkcs8.key")?);
    let keys = rustls_pemfile::private_key(key_file)?.unwrap();

    debug!("Loading TLS certificates");
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
    debug!("TLS configuration created successfully");

    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1885", move |_| {
            chain_factory(TlsAcceptor::new(tls_config.clone()))
                .map_err(|err| {
                    error!("TLS handshake failed: {}", err);
                    MqttError::Service(ServerError)
                })
                .and_then(fn_service(|io: Io<Layer<TlsServerFilter>>| {
                    // Handle peer certificate
                    let cert_info = io.query::<PeerCert>().as_ref().map(|cert: &PeerCert| {
                        X509Certificate::from_der(&cert.0)
                            .unwrap()
                            .1
                            .subject()
                            .to_string()
                    });
                    info!("Incoming TLS connection: peer certificate: {:?}", cert_info);
                    Ready::Ok(io)
                }))
                .and_then({
                    debug!("Initializing MQTT v3 server over TLS");
                    let mqtt_v3_server = v3::MqttServer::new(connect_v3)
                        .control(control_factory_v3())
                        .publish(publish_factory_v3())
                        .middleware(RequestLogger)
                        // .middleware(fn_pub_ack_factory_v3())
                        // .middleware(fn_handle_packet_id())
                        // .middleware(fn_auth)
                        .finish();

                    debug!("Initializing MQTT v5 server over TLS");
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
    // Initialize the logger
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("MQTT Gateway starting up");
    
    // Initialize upstream
    debug!("Initializing upstream connections");
    let _ = UPSTREAM.clone();
    
    info!("Starting MQTT servers");
    let tcp_handle = ntex::rt::spawn(listen_tcp());
    let tls_handle = ntex::rt::spawn(listen_tls());
    info!("All servers started, waiting for completion");
    let _ = tokio::join!(tcp_handle, tls_handle);
}
