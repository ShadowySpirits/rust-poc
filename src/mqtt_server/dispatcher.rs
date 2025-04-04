use super::error::ServerError;
use super::session::SessionState;
use ntex::service::fn_factory_with_config;
use ntex::util::Ready;
use ntex::{ServiceFactory, fn_service};
use ntex_mqtt::{v3, v5};
use std::cell::RefCell;

pub async fn connect_v3(
    mut handshake: v3::Handshake,
) -> Result<v3::HandshakeAck<SessionState>, ServerError> {
    let packet = handshake.packet_mut();
    let session_state = SessionState {
        client_id: packet.client_id.to_string(),
        subscriptions: RefCell::new(Vec::new()),
    };

    println!("new v3 connection: {:?}", handshake);
    Ok(handshake.ack(session_state, false))
}

pub fn control_factory_v3() -> impl ServiceFactory<
    v3::Control<ServerError>,
    v3::Session<SessionState>,
    Response = v3::ControlAck,
    Error = ServerError,
    InitError = ServerError,
> {
    fn_factory_with_config(|session: v3::Session<SessionState>| {
        Ready::Ok(fn_service(move |control| match control {
            v3::Control::Error(e) => Ready::Ok(e.ack()),
            v3::Control::ProtocolError(e) => Ready::Ok(e.ack()),
            v3::Control::Ping(p) => Ready::Ok(p.ack()),
            v3::Control::Disconnect(d) => Ready::Ok(d.ack()),
            v3::Control::Subscribe(mut s) => {
                // store subscribed topics in session, publish service uses this list for echos
                s.iter_mut().for_each(|mut s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    s.confirm(s.qos());
                });

                Ready::Ok(s.ack())
            }
            v3::Control::Unsubscribe(s) => Ready::Ok(s.ack()),
            v3::Control::Closed(c) => Ready::Ok(c.ack()),
            v3::Control::PeerGone(c) => Ready::Ok(c.ack()),
            // TODO: Back pressure
            v3::Control::WrBackpressure(w) => Ready::Ok(w.ack())
        }))
    })
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

pub fn control_factory_v5() -> impl ServiceFactory<
    v5::Control<ServerError>,
    v5::Session<SessionState>,
    Response = v5::ControlAck,
    Error = ServerError,
    InitError = ServerError,
> {
    fn_factory_with_config(|session: v5::Session<SessionState>| {
        Ready::Ok(fn_service(move |control| match control {
            v5::Control::Auth(a) => Ready::Ok(a.ack(v5::codec::Auth::default())),
            v5::Control::Error(e) => {
                Ready::Ok(e.ack(v5::codec::DisconnectReasonCode::UnspecifiedError))
            }
            v5::Control::ProtocolError(e) => Ready::Ok(e.ack()),
            v5::Control::Ping(p) => Ready::Ok(p.ack()),
            v5::Control::Disconnect(d) => Ready::Ok(d.ack()),
            v5::Control::Subscribe(mut s) => {
                // store subscribed topics in session, publish service uses this list for echos
                s.iter_mut().for_each(|mut s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    s.confirm(s.options().qos);
                });

                Ready::Ok(s.ack())
            }
            v5::Control::Unsubscribe(s) => Ready::Ok(s.ack()),
            v5::Control::Closed(c) => Ready::Ok(c.ack()),
            v5::Control::PeerGone(c) => Ready::Ok(c.ack()),
            // TODO: Back pressure
            v5::Control::WrBackpressure(w) => Ready::Ok(w.ack())
        }))
    })
}

pub fn publish_factory_v5() -> impl ServiceFactory<
    v5::Publish,
    v5::Session<SessionState>,
    Response = v5::PublishAck,
    Error = ServerError,
    InitError = ServerError,
> {
    fn_factory_with_config(|session: v5::Session<SessionState>| {
        Ready::Ok(fn_service(move |publish: v5::Publish| {
            println!(
                "incoming v5 publish from client: {:?}: {:?} -> {:?}",
                session.client_id,
                publish.id(),
                publish.topic()
            );
            Ready::Ok(publish.ack())
        }))
    })
}
