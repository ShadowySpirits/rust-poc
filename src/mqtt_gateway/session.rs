use ntex::util::{ByteString, Bytes};
use ntex_mqtt::{
    v3::{
        self, PublishBuilder, SubscribeBuilder, UnsubscribeBuilder,
        codec::SubscribeReturnCode,
        control::{Subscribe, Unsubscribe},
    },
    v5,
};
use std::{cell::RefCell, fmt};

use super::error::ServerError;

use super::dual::DualSink;

#[derive(Debug, Clone)]
pub struct SessionState<Source> {
    pub client_id: String,
    pub subscriptions: RefCell<Vec<ByteString>>,
    pub source: Source,
    pub sink: AnySink<Source>,
}

// TODO: implement separate logic for MQTT3 and MQTT5.
impl SessionState<v3::MqttSink> {}
impl SessionState<v5::MqttSink> {}

#[derive(Debug, Clone)]
pub enum AnySink<T> {
    MqttSink(T),
    DualSink(DualSink<T>),
}

impl AnySink<v3::MqttSink> {
    pub fn publish<U>(&self, topic: U, payload: Bytes) -> PublishBuilder
    where
        ByteString: From<U>,
    {
        match self {
            AnySink::MqttSink(sink) => sink.publish(topic, payload),
            AnySink::DualSink(sink) => sink.secondary_sink.publish(topic, payload),
        }
    }

    pub fn close(&self) {
        match self {
            AnySink::MqttSink(sink) => sink.close(),
            AnySink::DualSink(sink) => {
                sink.primary_sink.close();
                sink.secondary_sink.close();
            }
        }
    }

    pub async fn handle_subscribe(
        &self,
        session: &SessionState<v3::MqttSink>,
        mut s: Subscribe,
    ) -> Result<v3::ControlAck, ServerError> {
        match self {
            AnySink::MqttSink(sink) => {
                let subscribe_builder = s.iter_mut().fold(sink.subscribe(), |builder, s| {
                    session.subscriptions.borrow_mut().push(s.topic().clone());
                    builder.topic_filter(s.topic().clone(), s.qos())
                });

                subscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)
                    .map(|result| {
                        assert_eq!(result.len(), s.iter_mut().count());

                        s.iter_mut().zip(result.into_iter()).for_each(
                            |(mut sub, upstream_code)| match upstream_code {
                                SubscribeReturnCode::Success(qos) => sub.confirm(qos),
                                SubscribeReturnCode::Failure => sub.fail(),
                            },
                        );

                        s.ack()
                    })
            }
            AnySink::DualSink(sink) => {
                // TODO: handle subscribe for both primary and secondary sinks in parallel.
                let primary_subscribe_builder =
                    s.iter_mut()
                        .fold(sink.primary_sink.subscribe(), |builder, s| {
                            session.subscriptions.borrow_mut().push(s.topic().clone());
                            builder.topic_filter(s.topic().clone(), s.qos())
                        });

                primary_subscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)
                    .map(|result| {
                        assert_eq!(result.len(), s.iter_mut().count());

                        s.iter_mut().zip(result.into_iter()).for_each(
                            |(mut sub, upstream_code)| match upstream_code {
                                SubscribeReturnCode::Success(qos) => sub.confirm(qos),
                                SubscribeReturnCode::Failure => sub.fail(),
                            },
                        );
                    })?;

                let secondary_subscribe_builder =
                    s.iter_mut()
                        .fold(sink.secondary_sink.subscribe(), |builder, s| {
                            session.subscriptions.borrow_mut().push(s.topic().clone());
                            builder.topic_filter(s.topic().clone(), s.qos())
                        });

                secondary_subscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)
                    .map(|result| {
                        assert_eq!(result.len(), s.iter_mut().count());

                        s.iter_mut().zip(result.into_iter()).for_each(
                            |(mut sub, upstream_code)| match upstream_code {
                                SubscribeReturnCode::Success(qos) => sub.confirm(qos),
                                SubscribeReturnCode::Failure => sub.fail(),
                            },
                        );

                        s.ack()
                    })
            }
        }
    }

    pub async fn handle_unsubscribe(
        &self,
        session: &SessionState<v3::MqttSink>,
        s: Unsubscribe,
    ) -> Result<v3::ControlAck, ServerError> {
        // TODO: handle unsubscribe for both primary and secondary sinks in parallel.
        match self {
            AnySink::MqttSink(sink) => {
                let unsubscribe_builder = s.iter().fold(sink.unsubscribe(), |builder, topic| {
                    session.subscriptions.borrow_mut().push(topic.clone());
                    builder.topic_filter(topic.clone())
                });

                unsubscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)
                    .map(|_| s.ack())
            }
            AnySink::DualSink(sink) => {
                let primary_unsubscribe_builder = s.iter().fold(sink.primary_sink.unsubscribe(), |builder, topic| {
                    session.subscriptions.borrow_mut().push(topic.clone());
                    builder.topic_filter(topic.clone())
                });

                primary_unsubscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)?;

                let secondary_unsubscribe_builder = s.iter().fold(sink.secondary_sink.unsubscribe(), |builder, topic| {
                    session.subscriptions.borrow_mut().push(topic.clone());
                    builder.topic_filter(topic.clone())
                });

                secondary_unsubscribe_builder
                    .send()
                    .await
                    .map_err(|_| ServerError)
                    .map(|_| s.ack())
            }
        }
    }
}
