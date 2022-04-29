use crate::{
    client::ClientMessage,
    plugin::{Noop, Plugin},
    tree::SubscriptionTree,
};
use log::{info, warn};
use mqtt_v5::{
    topic::TopicFilter,
    types::{
        properties::{AssignedClientIdentifier, SessionExpiryInterval},
        AuthenticatePacket, ConnectAckPacket, ConnectPacket, ConnectReason, DisconnectReason,
        FinalWill, Packet, ProtocolVersion, PublishAckPacket, PublishAckReason,
        PublishCompletePacket, PublishCompleteReason, PublishPacket, PublishReceivedPacket,
        PublishReceivedReason, PublishReleasePacket, PublishReleaseReason, QoS, SubscribeAckPacket,
        SubscribeAckReason, SubscribePacket, UnsubscribeAckPacket, UnsubscribeAckReason,
        UnsubscribePacket,
    },
};
use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};
use tokio::sync::mpsc::{self, Receiver, Sender};

struct Session {
    #[allow(unused)]
    pub protocol_version: ProtocolVersion,
    // pub subscriptions: HashSet<SubscriptionTopic>,
    // pub shared_subscriptions: HashSet<SubscriptionTopic>,
    pub client_sender: Option<Sender<ClientMessage>>,

    // Used to unsubscribe from topics
    subscription_tokens: Vec<(TopicFilter, u64)>,

    // Keep track of outgoing packets with QoS 1 or 2
    outgoing_packets: Vec<PublishPacket>,

    // Keep track of outgoing PublishReceived packets.
    // TODO(bschwind) - Consider a HashSet if order isn't important.
    outgoing_publish_receives: Vec<u16>,

    // Keep track of outgoing PublishReleased packets.
    // TODO(bschwind) - Consider a HashSet if order isn't important.
    outgoing_publish_released: Vec<u16>,

    packet_counter: u16,

    session_expiry_interval: Option<Duration>,

    will: Option<FinalWill>,
}

impl Session {
    pub fn new(
        protocol_version: ProtocolVersion,
        will: Option<FinalWill>,
        session_expiry_interval: Option<Duration>,
        client_sender: Sender<ClientMessage>,
    ) -> Self {
        Self {
            protocol_version,
            // subscriptions: HashSet::new(),
            // shared_subscriptions: HashSet::new(),
            /// Tx handle for a connected client
            client_sender: Some(client_sender),
            subscription_tokens: Vec::new(),
            outgoing_packets: Vec::new(),
            outgoing_publish_receives: Vec::new(),
            outgoing_publish_released: Vec::new(),
            packet_counter: 1,
            session_expiry_interval,
            will,
        }
    }

    /// Turn self, an existing Session, into a new session suitable
    /// for a client which just connected.
    pub fn into_new_session(
        self,
        protocol_version: ProtocolVersion,
        will: Option<FinalWill>,
        session_expiry_interval: Option<Duration>,
        client_sender: Sender<ClientMessage>,
    ) -> Self {
        Self {
            protocol_version,
            client_sender: Some(client_sender),
            session_expiry_interval,
            will,
            ..self
        }
    }

    pub fn store_outgoing_publish(
        &mut self,
        mut publish: PublishPacket,
        subscriber_qos: QoS,
    ) -> u16 {
        assert!(subscriber_qos == QoS::AtLeastOnce || subscriber_qos == QoS::ExactlyOnce);

        // TODO(bschwind) - Prevent using packet IDs which already exist in `outgoing_packets`
        let packet_id = self.packet_counter;
        publish.packet_id = Some(self.packet_counter);
        publish.qos = subscriber_qos;
        publish.is_duplicate = false;
        self.packet_counter = self.packet_counter.wrapping_add(1);

        // Handle u16 wraparound, 0 is an invalid packet ID
        if self.packet_counter == 0 {
            self.packet_counter = self.packet_counter.wrapping_add(1);
        }

        self.outgoing_packets.push(publish);

        packet_id
    }

    pub fn remove_outgoing_publish(&mut self, packet_id: u16) {
        if let Some(pos) = self.outgoing_packets.iter().position(|p| p.packet_id == Some(packet_id))
        {
            let _packet = self.outgoing_packets.remove(pos);
        }
    }

    async fn resend_packets(&mut self) {
        if !self.outgoing_packets.is_empty() {
            let message = ClientMessage::Packets(
                self.outgoing_packets
                    .iter()
                    .cloned()
                    .map(|mut p| {
                        // Publish retries have their DUP flag set to true.
                        p.is_duplicate = true;
                        Packet::Publish(p)
                    })
                    .collect(),
            );
            self.send(message).await;
        }

        if !self.outgoing_publish_released.is_empty() {
            let message = ClientMessage::Packets(
                self.outgoing_publish_released
                    .iter()
                    .cloned()
                    .map(|p| {
                        Packet::PublishRelease(PublishReleasePacket {
                            packet_id: p,
                            reason_code: PublishReleaseReason::Success,
                            reason_string: None,
                            user_properties: vec![],
                        })
                    })
                    .collect(),
            );
            self.send(message).await;
        }
    }

    /// Attempt to send a `ClientMessage` to the client via the channel handle.
    /// If the channel is closed, the handle is removed from the session.
    async fn send(&mut self, message: ClientMessage) {
        if let Some(ref client_sender) = self.client_sender {
            if client_sender.send(message).await.is_err() {
                warn!("Failed to send message to client. Dropping sender");
                self.client_sender.take();
            }
        }
    }
}

#[derive(Debug)]
struct SessionSubscription {
    client_id: String,
    maximum_qos: QoS,
}

#[derive(Debug)]
pub enum WillDisconnectLogic {
    Send,
    DoNotSend,
}

#[derive(Debug)]
pub enum BrokerMessage {
    NewClient(Box<ConnectPacket>, Sender<ClientMessage>),
    Authenticate(String, AuthenticatePacket),
    Publish(String, Box<PublishPacket>),
    PublishAck(String, PublishAckPacket), // TODO - This can be handled by the client task
    PublishRelease(String, PublishReleasePacket), // TODO - This can be handled by the client task
    PublishReceived(String, PublishReceivedPacket),
    PublishComplete(String, PublishCompletePacket),
    PublishFinalWill(String, FinalWill),
    Subscribe(String, SubscribePacket), // TODO - replace string client_id with int
    Unsubscribe(String, UnsubscribePacket), // TODO - replace string client_id with int
    Disconnect(String, WillDisconnectLogic),
}

pub struct Broker<A> {
    sessions: HashMap<String, Session>,
    sender: Sender<BrokerMessage>,
    receiver: Receiver<BrokerMessage>,
    subscriptions: SubscriptionTree<SessionSubscription>,
    #[allow(unused)]
    plugin: A,
}

impl Default for Broker<Noop> {
    fn default() -> Self {
        Broker::<Noop>::new()
    }
}

impl<A: Plugin> Broker<A> {
    /// Construct a new Broker.
    pub fn new() -> Broker<Noop> {
        let (sender, receiver) = mpsc::channel(100);

        Broker {
            sessions: HashMap::new(),
            sender,
            receiver,
            subscriptions: SubscriptionTree::new(),
            plugin: Noop,
        }
    }

    /// Construct a new Broker.
    pub fn with_plugin(plugin: A) -> Broker<A> {
        let (sender, receiver) = mpsc::channel(100);

        Broker {
            sessions: HashMap::new(),
            sender,
            receiver,
            subscriptions: SubscriptionTree::new(),
            plugin,
        }
    }

    pub fn sender(&self) -> Sender<BrokerMessage> {
        self.sender.clone()
    }

    async fn take_over_existing_client(
        &mut self,
        client_id: &str,
        new_client_clean_start: bool,
    ) -> Option<Session> {
        let existing_session = if let Some(existing_session) = self.sessions.remove(client_id) {
            if let Some(client_sender) = &existing_session.client_sender {
                if let Err(e) = client_sender
                    .try_send(ClientMessage::Disconnect(DisconnectReason::SessionTakenOver))
                {
                    warn!("Failed to send disconnect packet to taken-over session - {:?}", e);
                }
            }

            // Publish the session's will if required.
            if let Some(will) = &existing_session.will {
                // If the Will Delay Interval of the existing Network Connection is 0 and
                // there is a Will Message, it will be sent because the Network Connection
                // is closed. If the Session Expiry Interval of the existing Network Connection
                // is 0, or the new Network Connection has Clean Start set to 1 then if the
                // existing Network Connection has a Will Message it will be sent because the
                // original Session is ended on the takeover.

                let should_send_will = will.will_delay_interval.is_none()
                    || existing_session.session_expiry_interval.is_none()
                    || new_client_clean_start;

                if should_send_will {
                    self.publish_message(will.clone().into()).await;
                }
            }

            Some(existing_session)
        } else {
            None
        };

        // If the Server accepts a connection with Clean Start set to 1, the Server MUST set Session Present to 0 in the
        // CONNACK packet in addition to setting a 0x00 (Success) Reason Code in the CONNACK packet [MQTT-3.2.2-2].
        // If the Server accepts a connection with Clean Start set to 0 and the Server has Session State for the ClientID,
        // it MUST set Session Present to 1 in the CONNACK packet, otherwise it MUST set Session Present to 0 in the CONNACK packet.
        if new_client_clean_start {
            return None;
        }

        existing_session
    }

    async fn handle_new_client(
        &mut self,
        connect_packet: ConnectPacket,
        client_msg_sender: Sender<ClientMessage>,
    ) {
        let mut takeover_session = self
            .take_over_existing_client(&connect_packet.client_id, connect_packet.clean_start)
            .await;
        let session_present = takeover_session.is_some();

        // TODO(flxo) Calling `resend_packets` after `take_over_existing_client` feels strange since a disconnect is sent in there.
        if let Some(existing_session) = &mut takeover_session {
            existing_session.resend_packets().await;
        }

        info!(
            "Client ID {} connected (Version: {:?})",
            connect_packet.client_id, connect_packet.protocol_version
        );

        let session_expiry_interval = match connect_packet.session_expiry_interval {
            Some(interval) => Some(interval),
            None if !connect_packet.clean_start => {
                // If the client passed "clean_start = false", we'll give them
                // a 10 minute session so they can go offline and still receive
                // messages.
                Some(SessionExpiryInterval(10 * 60))
            },
            None => None,
        };

        let connect_ack = ConnectAckPacket {
            // Variable header
            session_present,
            reason_code: ConnectReason::Success,

            // Properties
            session_expiry_interval,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            assigned_client_identifier: Some(AssignedClientIdentifier(
                connect_packet.client_id.clone(),
            )),
            topic_alias_maximum: None,
            reason_string: None,
            user_properties: vec![],
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };

        // A newly connected client should have empty channel and the queuing the connect ack *must* fit in the channel.
        client_msg_sender
            .send(ClientMessage::Packet(Packet::ConnectAck(connect_ack)))
            .await
            .expect("Failed to send Connect Acknowledgement");

        let session_expiry_duration =
            session_expiry_interval.map(|i| Duration::from_secs(i.0 as u64));

        let new_session = if let Some(existing_session) = takeover_session {
            let mut new_session = existing_session.into_new_session(
                connect_packet.protocol_version,
                connect_packet.will,
                session_expiry_duration,
                client_msg_sender,
            );

            new_session.resend_packets().await;

            new_session
        } else {
            Session::new(
                connect_packet.protocol_version,
                connect_packet.will,
                session_expiry_duration,
                client_msg_sender,
            )
        };

        self.sessions.insert(connect_packet.client_id, new_session);
    }

    async fn handle_subscribe(&mut self, client_id: String, packet: SubscribePacket) {
        let subscriptions = &mut self.subscriptions;

        if let Some(session) = self.sessions.get_mut(&client_id) {
            // If a Server receives a SUBSCRIBE packet containing a Topic Filter that
            // is identical to a Non‑shared Subscription’s Topic Filter for the current
            // Session, then it MUST replace that existing Subscription with a new Subscription.
            for topic in &packet.subscription_topics {
                let topic = &topic.topic_filter;
                // Unsubscribe the old session from all topics it subscribed to.
                session.subscription_tokens.retain(|(session_topic, token)| {
                    if *session_topic == *topic {
                        subscriptions.remove(session_topic, *token);
                        false
                    } else {
                        true
                    }
                });
            }

            // Iterate through each subscription, insert into the subscription tree,
            // and return the QoS that was granted.
            let granted_qos_values = packet
                .subscription_topics
                .into_iter()
                .map(|topic| {
                    let session_subscription = SessionSubscription {
                        client_id: client_id.clone(),
                        maximum_qos: topic.maximum_qos,
                    };
                    let token = subscriptions.insert(&topic.topic_filter, session_subscription);

                    session.subscription_tokens.push((topic.topic_filter.clone(), token));

                    match topic.maximum_qos {
                        QoS::AtMostOnce => SubscribeAckReason::GrantedQoSZero,
                        QoS::AtLeastOnce => SubscribeAckReason::GrantedQoSOne,
                        QoS::ExactlyOnce => SubscribeAckReason::GrantedQoSTwo,
                    }
                })
                .collect();

            let subscribe_ack = SubscribeAckPacket {
                packet_id: packet.packet_id,
                reason_string: None,
                user_properties: vec![],
                reason_codes: granted_qos_values,
            };

            session.send(ClientMessage::Packet(Packet::SubscribeAck(subscribe_ack))).await;
        }
    }

    async fn handle_unsubscribe(&mut self, client_id: String, packet: UnsubscribePacket) {
        let subscriptions = &mut self.subscriptions;

        if let Some(session) = self.sessions.get_mut(&client_id) {
            for filter in &packet.topic_filters {
                // Unsubscribe the old session from all topics it subscribed to.
                session.subscription_tokens.retain(|(session_topic, token)| {
                    if *session_topic == *filter {
                        subscriptions.remove(session_topic, *token);
                        false
                    } else {
                        true
                    }
                });
            }

            let reason_codes = packet
                .topic_filters
                .into_iter()
                .map(|filter| {
                    if let Some(pos) =
                        session.subscription_tokens.iter().position(|(topic, _)| filter == *topic)
                    {
                        let (topic, token) = session.subscription_tokens.remove(pos);
                        subscriptions.remove(&topic, token);
                        UnsubscribeAckReason::Success
                    } else {
                        UnsubscribeAckReason::NoSubscriptionExisted
                    }
                })
                .collect();

            let unsubscribe_ack = UnsubscribeAckPacket {
                packet_id: packet.packet_id,
                reason_string: None,
                user_properties: vec![],
                reason_codes,
            };

            session.send(ClientMessage::Packet(Packet::UnsubscribeAck(unsubscribe_ack))).await;
        }
    }

    fn handle_disconnect(&mut self, client_id: String, will_disconnect_logic: WillDisconnectLogic) {
        info!("Client ID {} disconnected", client_id);

        let mut disconnect_will = None;
        let mut session_expiry_duration = None;

        if let Entry::Occupied(mut session_entry) = self.sessions.entry(client_id.clone()) {
            session_entry.get_mut().client_sender.take();

            if let Some(expiry_interval) = session_entry.get().session_expiry_interval {
                // The Will Message MUST be published after the Network Connection is subsequently
                // closed and either the Will Delay Interval has elapsed or the Session ends, unless
                // the Will Message has been deleted by the Server on receipt of a DISCONNECT packet
                // with Reason Code 0x00 (Normal disconnection) or a new Network Connection for the
                // ClientID is opened before the Will Delay Interval has elapsed.
                if let Some(will) = &session_entry.get().will {
                    disconnect_will = Some(will.clone());
                    session_expiry_duration = Some(expiry_interval);
                }

            // TODO(bschwind) - Schedule the session to be deleted after the session expiry interval.
            } else {
                let session = session_entry.remove();

                // Unsubscribe the old session from all topics it subscribed to.
                for (topic, token) in session.subscription_tokens {
                    self.subscriptions.remove(&topic, token);
                }

                if let Some(will) = session.will {
                    disconnect_will = Some(will);
                }
            }

            // Schedule the will to be sent
            if let Some(will) = disconnect_will {
                match will_disconnect_logic {
                    WillDisconnectLogic::Send => {
                        let will_send_delay_duration =
                            session_expiry_duration.unwrap_or_else(|| Duration::from_secs(0)).min(
                                will.will_delay_duration()
                                    .unwrap_or_else(|| Duration::from_secs(0)),
                            );
                        let broker_sender = self.sender.clone();

                        // Spawn a task that publishes the will after `will_send_delay_duration`
                        tokio::spawn(async move {
                            tokio::time::sleep(will_send_delay_duration).await;
                            broker_sender
                                .send(BrokerMessage::PublishFinalWill(client_id, will))
                                .await
                                .expect("Failed to send final will message to broker");
                        });
                    },
                    WillDisconnectLogic::DoNotSend => {},
                }
            }
        }
    }

    async fn publish_message(&mut self, packet: PublishPacket) {
        let topic = &packet.topic;
        let sessions = &mut self.sessions;

        for session_subscription in self.subscriptions.matching_subscribers(topic) {
            if let Some(session) = sessions.get_mut(&session_subscription.client_id) {
                let outgoing_packet_id = match session_subscription.maximum_qos {
                    QoS::AtLeastOnce | QoS::ExactlyOnce => {
                        Some(session.store_outgoing_publish(
                            packet.clone(),
                            session_subscription.maximum_qos,
                        ))
                    },
                    _ => None,
                };

                let outgoing_packet = PublishPacket {
                    packet_id: outgoing_packet_id,
                    qos: session_subscription.maximum_qos,
                    is_duplicate: false,
                    ..packet.clone()
                };

                session.send(ClientMessage::Packet(Packet::Publish(outgoing_packet))).await;
            }
        }
    }

    async fn handle_publish(&mut self, client_id: String, packet: PublishPacket) {
        let mut is_dup = false;

        // For QoS2, ensure this packet isn't delivered twice. So if we have an outgoing
        // publish receive with the same ID, just send the publish receive again but don't forward
        // the message.
        match packet.qos {
            QoS::AtMostOnce => {},
            QoS::AtLeastOnce => {
                if let Some(session) = self.sessions.get_mut(&client_id) {
                    let publish_ack = PublishAckPacket {
                        packet_id: packet
                            .packet_id
                            .expect("Packet with QoS 1 should have a packet ID"),
                        reason_code: PublishAckReason::Success,
                        reason_string: None,
                        user_properties: vec![],
                    };

                    session.send(ClientMessage::Packet(Packet::PublishAck(publish_ack))).await;
                }
            },
            QoS::ExactlyOnce => {
                if let Some(session) = self.sessions.get_mut(&client_id) {
                    let packet_id = packet.packet_id.unwrap();
                    is_dup = session.outgoing_publish_receives.contains(&packet_id);

                    if !is_dup {
                        session.outgoing_publish_receives.push(packet_id)
                    }

                    let publish_recv = PublishReceivedPacket {
                        packet_id: packet
                            .packet_id
                            .expect("Packet with QoS 2 should have a packet ID"),
                        reason_code: PublishReceivedReason::Success,
                        reason_string: None,
                        user_properties: vec![],
                    };

                    session
                        .send(ClientMessage::Packet(Packet::PublishReceived(publish_recv)))
                        .await;
                }
            },
        }

        if !is_dup {
            self.publish_message(packet).await;
        }
    }

    fn handle_publish_ack(&mut self, client_id: String, packet: PublishAckPacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.remove_outgoing_publish(packet.packet_id);
        }
    }

    async fn handle_publish_release(&mut self, client_id: String, packet: PublishReleasePacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if let Some(pos) =
                session.outgoing_publish_receives.iter().position(|x| *x == packet.packet_id)
            {
                session.outgoing_publish_receives.remove(pos);

                let outgoing_packet = PublishCompletePacket {
                    packet_id: packet.packet_id,
                    reason_code: PublishCompleteReason::Success,
                    reason_string: None,
                    user_properties: vec![],
                };

                session.send(ClientMessage::Packet(Packet::PublishComplete(outgoing_packet))).await;
            }
        }
    }

    async fn handle_publish_received(&mut self, client_id: String, packet: PublishReceivedPacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if let Some(pos) = session.outgoing_packets.iter().position(|p| {
                p.qos == QoS::ExactlyOnce
                    && p.packet_id.map(|id| id == packet.packet_id).unwrap_or(false)
            }) {
                // TODO(bschwind) - remove it here?
                session.outgoing_packets.remove(pos);

                session.outgoing_publish_released.push(packet.packet_id);

                let outgoing_packet = PublishReleasePacket {
                    packet_id: packet.packet_id,
                    reason_code: PublishReleaseReason::Success,
                    reason_string: None,
                    user_properties: vec![],
                };

                session.send(ClientMessage::Packet(Packet::PublishRelease(outgoing_packet))).await;
            }
        }
    }

    fn handle_publish_complete(&mut self, client_id: String, packet: PublishCompletePacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if let Some(pos) =
                session.outgoing_publish_released.iter().position(|x| *x == packet.packet_id)
            {
                session.outgoing_publish_released.remove(pos);
            }
        }
    }

    async fn publish_final_will(&mut self, client_id: String, final_will: FinalWill) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            if session.client_sender.is_some() {
                // They've reconnected, don't send out the will message.
                self.publish_message(final_will.into()).await;
            } else {
                // They haven't reconnected, send out the will message.
            }
        } else {
            // No existing session, send out the will message.
            self.publish_message(final_will.into()).await;
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                BrokerMessage::NewClient(connect_packet, client_msg_sender) => {
                    self.handle_new_client(*connect_packet, client_msg_sender).await;
                },
                BrokerMessage::Subscribe(client_id, packet) => {
                    self.handle_subscribe(client_id, packet).await;
                },
                BrokerMessage::Unsubscribe(client_id, packet) => {
                    self.handle_unsubscribe(client_id, packet).await;
                },
                BrokerMessage::Disconnect(client_id, will_disconnect_logic) => {
                    self.handle_disconnect(client_id, will_disconnect_logic);
                },
                BrokerMessage::Publish(client_id, packet) => {
                    self.handle_publish(client_id, *packet).await;
                },
                BrokerMessage::PublishAck(client_id, packet) => {
                    self.handle_publish_ack(client_id, packet);
                },
                BrokerMessage::PublishRelease(client_id, packet) => {
                    self.handle_publish_release(client_id, packet).await;
                },
                BrokerMessage::PublishReceived(client_id, packet) => {
                    self.handle_publish_received(client_id, packet).await;
                },
                BrokerMessage::PublishComplete(client_id, packet) => {
                    self.handle_publish_complete(client_id, packet);
                },
                BrokerMessage::PublishFinalWill(client_id, final_will) => {
                    self.publish_final_will(client_id, final_will).await;
                },
                BrokerMessage::Authenticate(client_id, _) => {
                    warn!("Ignoring unexpected authentication message from {}", client_id);
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        broker::{Broker, BrokerMessage},
        client::ClientMessage,
        plugin::Noop,
    };
    use mqtt_v5::types::{properties::*, ProtocolVersion, *};
    use tokio::{
        runtime::Runtime,
        sync::mpsc::{self, Sender},
    };

    async fn run_client(broker_tx: Sender<BrokerMessage>) {
        let (sender, mut receiver) = mpsc::channel(5);

        let connect_packet = ConnectPacket {
            protocol_name: "mqtt".to_string(),
            protocol_version: ProtocolVersion::V500,
            clean_start: true,
            keep_alive: 1000,

            session_expiry_interval: None,
            receive_maximum: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            request_response_information: None,
            request_problem_information: None,
            user_properties: vec![],
            authentication_method: None,
            authentication_data: None,

            client_id: "TEST".to_string(),
            will: None,
            user_name: None,
            password: None,
        };

        let _ = broker_tx
            .send(BrokerMessage::NewClient(Box::new(connect_packet), sender))
            .await
            .unwrap();

        let resp = receiver.recv().await.unwrap();

        assert_eq!(
            resp,
            ClientMessage::Packet(Packet::ConnectAck(ConnectAckPacket {
                session_present: false,
                reason_code: ConnectReason::Success,

                session_expiry_interval: None,
                receive_maximum: None,
                maximum_qos: None,
                retain_available: None,
                maximum_packet_size: None,
                assigned_client_identifier: Some(AssignedClientIdentifier("TEST".to_string())),
                topic_alias_maximum: None,
                reason_string: None,
                user_properties: vec![],
                wildcard_subscription_available: None,
                subscription_identifiers_available: None,
                shared_subscription_available: None,
                server_keep_alive: None,
                response_information: None,
                server_reference: None,
                authentication_method: None,
                authentication_data: None,
            }))
        );

        let _ = broker_tx
            .send(BrokerMessage::Subscribe(
                "TEST".to_string(),
                SubscribePacket {
                    packet_id: 0,
                    subscription_identifier: None,
                    user_properties: vec![],
                    subscription_topics: vec![SubscriptionTopic {
                        topic_filter: "home/kitchen/temperature".parse().unwrap(),
                        maximum_qos: QoS::AtMostOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: RetainHandling::DoNotSend,
                    }],
                },
            ))
            .await
            .unwrap();

        let resp = receiver.recv().await.unwrap();

        assert_eq!(
            resp,
            ClientMessage::Packet(Packet::SubscribeAck(SubscribeAckPacket {
                packet_id: 0,
                reason_string: None,
                user_properties: vec![],
                reason_codes: vec![SubscribeAckReason::GrantedQoSZero,],
            }))
        );
    }

    #[test]
    fn simple_client_test() {
        let broker = Broker::<Noop>::new();
        let sender = broker.sender();

        let runtime = Runtime::new().unwrap();

        runtime.spawn(broker.run());
        runtime.block_on(run_client(sender));
    }
}
