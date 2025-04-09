use ntex::util::ByteString;
use ntex_mqtt::{v3, v5};
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct SessionState<Channel> {
    pub client_id: String,
    pub subscriptions: RefCell<Vec<ByteString>>,
    pub source: Channel,
    pub sink: Channel,
}

// TODO: implement separate logic for MQTT3 and MQTT5.
impl SessionState<v3::MqttSink> {}
impl SessionState<v5::MqttSink> {}
