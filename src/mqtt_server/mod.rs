use ntex_mqtt::{v3, v5, MqttServer};

mod dispatcher;
mod session;
mod error;

async fn serve() -> std::io::Result<()> {
    ntex::server::Server::build()
        .bind("mqtt-server", "127.0.0.1:1883", |_config| {
            let mqtt_v3_server = v3::MqttServer::new(dispatcher::connect_v3)
                .publish(dispatcher::publish_factory_v3())
                .finish();

            let mqtt_v5_server = v5::MqttServer::new(dispatcher::connect_v5)
                .publish(dispatcher::publish_v5)
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