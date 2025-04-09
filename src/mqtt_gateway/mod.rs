use ntex_mqtt::{v3, v5, MqttServer};
use crate::dispatcher::{connect_v3, connect_v5, control_factory_v3, control_factory_v5, publish_factory_v3, publish_factory_v5};

mod dispatcher;
mod session;
mod error;
mod upstream;
mod handler;

async fn serve() -> std::io::Result<()> {
    ntex::server::Server::build()
        .bind("mqtt-gateway", "0.0.0.0:1884", |_config| {
            let mqtt_v3_server = v3::MqttServer::new(connect_v3)
                .control(control_factory_v3())
                .publish(publish_factory_v3())
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