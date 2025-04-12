use ntex::codec::{Decoder, Encoder};
use ntex::{Middleware, Service, ServiceCtx};
use ntex_io::DispatchItem;
use ntex_mqtt::error::{DecodeError, EncodeError};
use ntex_mqtt::v3;
use std::rc::Rc;

// (MQTT packet, Remaining length)
type DecodeResult = (v3::codec::Packet, u32);

pub struct SayHi;

impl<S> Middleware<S> for SayHi {
    type Service = SayHiMiddleware<S>;

    fn create(&self, service: S) -> Self::Service {
        SayHiMiddleware { service }
    }
}

pub struct SayHiMiddleware<S> {
    service: S,
}

impl<S, I> Service<DispatchItem<Rc<I>>> for SayHiMiddleware<S>
where
    S: Service<DispatchItem<Rc<I>>>,
    I: Decoder<Item=DecodeResult, Error=DecodeError> + Encoder<Item=v3::codec::Packet, Error=EncodeError>
{
    type Response = S::Response;
    type Error = S::Error;

    ntex::forward_ready!(service);

    async fn call(&self, req: DispatchItem<Rc<I>>, ctx: ServiceCtx<'_, Self>) -> Result<Self::Response, Self::Error> {
        println!("Hi from start. You requested: {:?}", req);
        let res = ctx.call(&self.service, req).await?;
        println!("Hi from response");
        Ok(res)
    }
}

