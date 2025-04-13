use crate::error::ServerError;
use crate::session::SessionState;
use crate::upstream::create_lb;
use ntex::fn_service;
use ntex::time::Seconds;
use ntex_mqtt::v3::ControlAck;
use ntex_mqtt::v3::client::Control;
use ntex_mqtt::{QoS, v3};
use std::cell::RefCell;

pub(crate) async fn handle_connect(
    mut handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState<v3::MqttSink>>, ServerError> {
    // TODO: verify the connect packet.
    let packet = handshake.packet_mut();

    let backend = create_lb()
        .await
        .select(packet.client_id.as_slice(), 1)
        .unwrap();

    // TODO: clone the received connect packet.
    let client = v3::client::MqttConnector::new(backend.addr.to_string())
        .client_id(packet.client_id.to_string())
        .keep_alive(Seconds::new(60))
        .connect()
        .await
        .unwrap();

    // TODO: close connection when source is disconnected.
    let upstream_sink = client.sink();

    // TODO: load session from database.
    let session_state = SessionState {
        client_id: packet.client_id.to_string(),
        subscriptions: RefCell::new(Vec::new()),
        source: handshake.sink(),
        // TODO: create multiple sinks if they need to connect to multiple upstream.
        sink: upstream_sink,
    };

    let session_clone = session_state.clone();
    ntex::rt::spawn_fn(move || {
        client.start(fn_service(
            move |packet: Control<ServerError>| match packet {
                Control::Publish(publish) => handle_upstream_pub(publish, session_clone.clone()),
                _ => {
                    println!(
                        "Receive packet from upstream but not implement: {:?}",
                        packet
                    );
                    unimplemented!()
                }
            },
        ))
    });

    println!("new v3 connection: {:?}", handshake);
    Ok(handshake.ack(session_state, false))
}

pub(crate) async fn handle_downstream_pub(
    mut publish: v3::Publish,
    session: SessionState<v3::MqttSink>,
) -> Result<(), ServerError> {
    println!(
        "incoming v3 publish from client: {:?}: {:?} -> {:?}",
        session.client_id,
        publish.id(),
        publish.topic()
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
) -> Result<ControlAck, ServerError> {
    println!(
        "incoming v3 publish from backend: {:?} -> {:?}",
        publish.packet().packet_id,
        publish.packet().topic
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
