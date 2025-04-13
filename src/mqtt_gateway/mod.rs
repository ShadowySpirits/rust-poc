use crate::dispatcher::{
    connect_v3, connect_v5, control_factory_v3, control_factory_v5, publish_factory_v3,
    publish_factory_v5,
};
use crate::middleware::RequestLogger;
use ntex_mqtt::{MqttServer, v3, v5};

mod dispatcher;
mod error;
mod handler;
mod middleware;
mod session;
mod upstream;

async fn serve() -> std::io::Result<()> {
    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1884", |_config| {
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

#[ntex::main]
async fn main() {
    serve().await.unwrap()
}
