use ntex::service::fn_factory_with_config;
use ntex::util::Ready;
use ntex::{ServiceFactory, fn_service};
use ntex_mqtt::{v3, v5};
use super::error::ServerError;
use super::session::SessionState;

pub async fn connect_v3(
    mut handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState>, ServerError> {
    let packet = handshake.packet_mut();
    let session_state = SessionState {
        client_id: packet.client_id.to_string(),
    };

    println!("new v3 connection: {:?}", handshake);
    Ok(handshake.ack(session_state, false))
}

pub fn publish_factory_v3() -> impl ServiceFactory<
    v3::Publish,
    v3::Session<SessionState>,
    Response = (),
    Error = ServerError,
    InitError = ServerError,
> {
    fn_factory_with_config(|session: v3::Session<SessionState>| {
        Ready::Ok(fn_service(move |publish: v3::Publish| {
            println!(
                "incoming v3 publish from client: {:?}: {:?} -> {:?}",
                session.client_id,
                publish.id(),
                publish.topic()
            );
            Ready::Ok(())
        }))
    })
}

pub async fn connect_v5(
    handshake: v5::Handshake,
) -> Result<v5::HandshakeAck<SessionState>, ServerError> {
    println!("new v5 connection: {:?}", handshake);
    Ok(handshake.ack(SessionState::default()))
}

pub async fn publish_v5(publish: v5::Publish) -> Result<v5::PublishAck, ServerError> {
    println!(
        "incoming v5 publish: {:?} -> {:?}",
        publish.id(),
        publish.topic()
    );
    Ok(publish.ack())
}
