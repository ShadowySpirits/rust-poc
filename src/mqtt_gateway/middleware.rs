use ntex::codec::{Decoder, Encoder};
use ntex::{Middleware, Service, ServiceCtx};
use ntex_io::DispatchItem;
use ntex_mqtt::error::{DecodeError, EncodeError};
use ntex_mqtt::v3;
use std::rc::Rc;

// (MQTT packet, Remaining length)
type DecodeResult = (v3::codec::Packet, u32);

pub struct RequestLogger;

impl<S> Middleware<S> for RequestLogger {
    type Service = RequestLoggerImpl<S>;

    fn create(&self, service: S) -> Self::Service {
        RequestLoggerImpl { service }
    }
}

pub struct RequestLoggerImpl<S> {
    service: S,
}

impl<S, I> Service<DispatchItem<Rc<I>>> for RequestLoggerImpl<S>
where
    S: Service<DispatchItem<Rc<I>>, Response = Option<v3::codec::Packet>>,
    I: Decoder<Item = DecodeResult, Error = DecodeError>
        + Encoder<Item = v3::codec::Packet, Error = EncodeError>,
{
    type Response = Option<v3::codec::Packet>;
    type Error = S::Error;

    ntex::forward_poll!(service);
    ntex::forward_ready!(service);
    ntex::forward_shutdown!(service);

    async fn call(
        &self,
        req: DispatchItem<Rc<I>>,
        ctx: ServiceCtx<'_, Self>,
    ) -> Result<Self::Response, Self::Error> {
        // TODO: get session state.
        println!("Receive MQTT packet: {:?}", req);
        let res = ctx.call(&self.service, req).await?;
        println!("Send MQTT packet: {:?}", res);
        Ok(res)
    }
}
