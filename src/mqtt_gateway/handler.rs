use crate::error::ServerError;
use crate::session::SessionState;
use crate::upstream::create_lb;
use ntex::fn_service;
use ntex::time::Seconds;
use ntex_mqtt::v3::codec::SubscribeReturnCode;
use ntex_mqtt::{QoS, v3};
use std::cell::RefCell;

pub(crate) async fn handle_connect(
    mut handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState<v3::MqttSink>>, ServerError> {
    // TODO: verify the connect packet.
    let packet = handshake.packet_mut();

    // TODO: get client certificate
    // handshake.io().query::<PeerCert>().as_ref();

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

    let handle_upstream = |packet: v3::client::Control<ServerError>,
                           session: SessionState<v3::MqttSink>| async {
        match packet {
            v3::client::Control::Publish(publish) => handle_upstream_pub(publish, session).await,
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

    println!("new v3 connection: {:?}", handshake);
    Ok(handshake.ack(session_state, false))
}

pub(crate) async fn handle_downstream_pub(
    mut publish: v3::Publish,
    session: SessionState<v3::MqttSink>,
) -> Result<(), ServerError> {
    println!(
        "incoming v3 publish from client: {}: ({:?}, {:?})",
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
    println!(
        "incoming v3 publish from backend: ({:?}, {}) -> {}",
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
            let subscribe_builder =
                s.iter_mut()
                    .fold(session.sink.subscribe(), |builder, s| {
                        session.subscriptions.borrow_mut().push(s.topic().clone());
                        builder.topic_filter(s.topic().clone(), s.qos())
                    });

            subscribe_builder
                .send()
                .await
                .map_err(|_| ServerError)
                .map(|result| {
                    assert_eq!(result.len(), s.iter_mut().count());

                    s.iter_mut()
                        .zip(result.into_iter())
                        .for_each(|(mut sub, upstream_code)| match upstream_code {
                            SubscribeReturnCode::Success(qos) => sub.confirm(qos),
                            SubscribeReturnCode::Failure => sub.fail(),
                        });

                    s.ack()
                })
        }
        v3::Control::Unsubscribe(s) => {
            let unsubscribe_builder =
                s.iter()
                    .fold(session.sink.unsubscribe(), |builder, topic| {
                        session.subscriptions.borrow_mut().push(topic.clone());
                        builder.topic_filter(topic.clone())
                    });

            unsubscribe_builder
                .send()
                .await
                .map_err(|_| ServerError)
                .map(|_| s.ack())
        },
        v3::Control::Error(e) => Ok(e.ack()),
        v3::Control::ProtocolError(e) => Ok(e.ack()),
        v3::Control::Ping(p) => Ok(p.ack()),
        v3::Control::Disconnect(d) => {
            println!("Receive downstream disconnect packet: clientId: {}, {:?}", session.client_id, d);
            session.sink.close();
            Ok(d.ack())
        },
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
            println!("Server error: clientId: {}, {:?}", session.client_id, error.get_ref());
            Ok(error.ack())
        }
        v3::client::Control::ProtocolError(error) => {
            session.source.close();
            println!("Protocol error: clientId: {}, {}", session.client_id, error.get_ref());
            Ok(error.ack())
        }
        v3::client::Control::PeerGone(p) => {
            session.source.close();
            println!("Peer gone error: clientId: {}, {:?}", session.client_id, p.err());
            Ok(p.ack())
        }
        _ => unimplemented!(),
    }
}
