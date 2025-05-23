use super::session::AnySink;

use super::dual::DualSink;

use super::UPSTREAM;
use super::error::ServerError;
use super::session::SessionState;
use log::{debug, error, info};
use ntex::fn_service;
use ntex::time::Seconds;
use ntex_mqtt::v3::codec::SubscribeReturnCode;
use ntex_mqtt::{QoS, v3};
use std::cell::RefCell;
use std::env;

pub(crate) async fn handle_connect(
    mut handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState<v3::MqttSink>>, ServerError> {

    if env::var("RUN_DUAL").is_ok() {
        return handle_dual_connect(handshake).await;
    }
    // TODO: verify the connect packet.
    let client_id = handshake.packet_mut().client_id.to_string();

    // TODO: get client certificate
    // handshake.io().query::<PeerCert>().as_ref();

    let backend = UPSTREAM.select(client_id.as_bytes(), 1).ok_or_else(|| {
        error!("No backend found for client ID: {}", client_id);
        ServerError
    })?;

    // TODO: clone the received connect packet.
    let client = v3::client::MqttConnector::new(backend.addr.to_string())
        .client_id(client_id.clone())
        .keep_alive(Seconds::new(60))
        .connect()
        .await
        .map_err(|e| {
            error!("TCP connection to backend {} failed: {}", backend.addr, e);
            ServerError
        })?;

    // TODO: close connection when source is disconnected.
    let upstream_sink = client.sink();

    // TODO: load session from database.
    let sink = handshake.sink();
    let session_state = SessionState {
        client_id: client_id.clone(),
        subscriptions: RefCell::new(Vec::new()),
        source: sink,
        // TODO: create multiple sinks if they need to connect to multiple upstream.
        sink: AnySink::MqttSink(upstream_sink),
    };

    let handle_upstream =
        |packet: v3::client::Control<ServerError>,
         session: SessionState<v3::MqttSink>| async {
            match packet {
                v3::client::Control::Publish(publish) => {
                    handle_upstream_pub(publish, session).await
                }
                _ => handle_upstream_control(packet, session).await,
            }
        };

    let session_clone = session_state.clone();
    ntex::rt::spawn_fn(move || {
        client.start(fn_service(
            move |packet: v3::client::Control<ServerError>| {
                handle_upstream(packet, session_clone.clone())
            },
        ))
    });

    info!(
        "New MQTT v3 TCP connection established: client_id={}",
        client_id
    );
    debug!("Connection details: handshake received");
    Ok(handshake.ack(session_state, false))
}

pub(crate) async fn handle_downstream_pub(
    mut publish: v3::Publish,
    session: SessionState<v3::MqttSink>,
) -> Result<(), ServerError> {
    debug!(
        "Incoming MQTT v3 publish over TCP from client {}: packet_id={:?}, topic={:?}",
        session.client_id,
        publish.id(),
        publish.topic(),
    );

    // Forward duplicate downstream packets to the backend.
    let new_packet_builder = session
        .sink
        .publish(publish.topic().get_ref().clone(), publish.take_payload());

    if let QoS::AtMostOnce = publish.packet().qos {
        new_packet_builder
            .send_at_most_once()
            .map_err(|_| ServerError)
    } else {
        // TODO: spawn a task to schedule retry.
        // Wait for PUBACK
        new_packet_builder
            .send_at_least_once()
            .await
            .map_err(|_| ServerError)
    }
}



async fn handle_upstream_pub(
    publish: v3::client::control::Publish,
    session: SessionState<v3::MqttSink>,
) -> Result<v3::ControlAck, ServerError> {
    debug!(
        "Incoming MQTT v3 publish over TCP from backend: packet_id={:?}, topic={} -> client_id={}",
        publish.packet().packet_id,
        publish.packet().topic,
        session.client_id
    );

    // TODO: Return a specific error for duplicate publish attempts, preventing the release of inflight counter.
    if publish.packet().dup {
        return Err(ServerError);
    }

    let new_packet_builder = session.source.publish(
        publish.packet().topic.clone(),
        publish.packet().payload.clone(),
    );

    if let QoS::AtMostOnce = publish.packet().qos {
        new_packet_builder
            .send_at_most_once()
            .map(|_| publish.ack())
            .map_err(|_| ServerError)
    } else {
        // TODO: spawn a task to schedule retry.
        // Wait for PUBACK
        new_packet_builder
            .send_at_least_once()
            .await
            .map(|_| publish.ack())
            .map_err(|_| ServerError)
    }
}

pub(crate) async fn handle_downstream_control(
    control: v3::Control<ServerError>,
    session: SessionState<v3::MqttSink>,
) -> Result<v3::ControlAck, ServerError> {
    match control {
        v3::Control::Subscribe(mut s) => {
            session.sink.handle_subscribe(&session, s).await
        }
        v3::Control::Unsubscribe(s) => {
            session.sink.handle_unsubscribe(&session, s).await
        }
        v3::Control::Error(e) => Ok(e.ack()),
        v3::Control::ProtocolError(e) => Ok(e.ack()),
        v3::Control::Ping(p) => Ok(p.ack()),
        v3::Control::Disconnect(d) => {
            info!(
                "Received TCP disconnect from client: client_id={}",
                session.client_id
            );
            debug!("Disconnect details: {:?}", d);
            session.sink.close();
            Ok(d.ack())
        }
        v3::Control::Closed(c) => Ok(c.ack()),
        v3::Control::PeerGone(c) => Ok(c.ack()),
        // TODO: Back pressure
        v3::Control::WrBackpressure(w) => Ok(w.ack()),
    }
}

pub(crate) async fn handle_upstream_control(
    control: v3::client::Control<ServerError>,
    session: SessionState<v3::MqttSink>,
) -> Result<v3::ControlAck, ServerError> {
    match control {
        v3::client::Control::Closed(c) => {
            session.source.close();
            Ok(c.ack())
        }
        v3::client::Control::Error(error) => {
            session.source.close();
            println!(
                "Server error: clientId: {}, {:?}",
                session.client_id,
                error.get_ref()
            );
            Ok(error.ack())
        }
        v3::client::Control::ProtocolError(error) => {
            session.source.close();
            println!(
                "Protocol error: clientId: {}, {}",
                session.client_id,
                error.get_ref()
            );
            Ok(error.ack())
        }
        v3::client::Control::PeerGone(p) => {
            session.source.close();
            println!(
                "Peer gone error: clientId: {}, {:?}",
                session.client_id,
                p.err()
            );
            Ok(p.ack())
        }
        _ => unimplemented!(),
    }
}

pub(crate) async fn handle_dual_connect(
    handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState<v3::MqttSink>>, ServerError> {
    // TODO initialize dual sink
    let client_id = handshake.packet().client_id.to_string();
    let primary_sink_address = "127.0.0.1:1883".to_string();
    let secondary_sink_address = "127.0.0.1:2883".to_string();
    
    
    // TODO: clone the received connect packet.
    let primary_client = v3::client::MqttConnector::new(primary_sink_address.clone())
        .client_id(client_id.clone())
        .keep_alive(Seconds::new(60))
        .connect()
        .await
        .map_err(|e| {
            error!("TCP connection to backend {} failed: {}", primary_sink_address, e);
            ServerError
        })?;

    let secondary_client = v3::client::MqttConnector::new(secondary_sink_address.clone())
        .client_id(client_id.clone())
        .keep_alive(Seconds::new(60))
        .connect()
        .await
        .map_err(|e| {
            error!("TCP connection to backend {} failed: {}", secondary_sink_address, e);
            ServerError
        })?;

    let dual_sink = DualSink::new(client_id.clone(), primary_client.sink(), secondary_client.sink());
    let source_sink = handshake.sink();

    let session_state = SessionState {
        client_id: client_id.clone(),
        subscriptions: RefCell::new(Vec::new()),
        source: source_sink,
        sink: AnySink::DualSink(dual_sink),
    };



   
    let handle_upstream =
        |packet: v3::client::Control<ServerError>,
         session: SessionState<v3::MqttSink>| async {
            match packet {
                v3::client::Control::Publish(publish) => {
                    handle_dual_upstream_pub(publish, session).await
                }
                _ => handle_upstream_control(packet, session).await,
            }
        };

    let session_clone1 = session_state.clone();
    let session_clone2 = session_state.clone();
    ntex::rt::spawn_fn(move || {
        primary_client.start(fn_service(
            move |packet: v3::client::Control<ServerError>| {
                handle_upstream(packet, session_clone1.clone())
            },
        ))
    });

    ntex::rt::spawn_fn(move || {
        secondary_client.start(fn_service(
            move |packet: v3::client::Control<ServerError>| {
                handle_upstream(packet, session_clone2.clone())
            },
        ))
    });

    info!(
        "New MQTT v3 TCP connection established: client_id={}",
        client_id
    );
    debug!("Connection details: handshake received");
    Ok(handshake.ack(session_state, false))
}




async fn handle_dual_upstream_pub(
    publish: v3::client::control::Publish,
    session: SessionState<v3::MqttSink >,
) -> Result<v3::ControlAck, ServerError> {
    debug!(
        "Incoming MQTT v3 publish over TCP from backend: packet_id={:?}, topic={} -> client_id={}",
        publish.packet().packet_id,
        publish.packet().topic,
        session.client_id
    );

    // TODO: Return a specific error for duplicate publish attempts, preventing the release of inflight counter.
    if publish.packet().dup {
        return Err(ServerError);
    }

    let new_packet_builder = session.source.publish(
        publish.packet().topic.clone(),
        publish.packet().payload.clone(),
    );

    if let QoS::AtMostOnce = publish.packet().qos {
        new_packet_builder
            .send_at_most_once()
            .map(|_| publish.ack())
            .map_err(|_| ServerError)
    } else {
        // TODO: spawn a task to schedule retry.
        // Wait for PUBACK
        new_packet_builder
            .send_at_least_once()
            .await
            .map(|_| publish.ack())
            .map_err(|_| ServerError)
    }
}
