use std::cell::RefCell;
use std::sync::Arc;

use super::error::ServerError;
use super::session::SessionState;
use log::error;
use ntex::fn_service;
use ntex::time::Seconds;
use ntex_mqtt::v3;

#[derive(Clone,Debug)]
pub struct DualSink<S> {
    pub client_id: String,
    pub primary_sink: S,
    pub secondary_sink: S,
}


impl DualSink<v3::MqttSink> {
    pub fn new(
        client_id: String,
        primary_sink: v3::MqttSink,
        secondary_sink: v3::MqttSink,
    ) -> Self {
        Self {
            client_id,
            primary_sink,
            secondary_sink,
        }
    }
}