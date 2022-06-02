use crate::{
    client::ClientMessage,
    plugin::{AuthentificationResult, Noop, Plugin},
    tree::SubscriptionTree,
};
use log::{debug, info, warn};
use mqtt_v5::{
    topic::TopicFilter,
    types::{
        properties::{AssignedClientIdentifier, SessionExpiryInterval},
        AuthenticatePacket, ConnectAckPacket, ConnectPacket, ConnectReason, DisconnectReason,
        FinalWill, Packet, ProtocolVersion, PublishAckPacket, PublishCompletePacket,
        PublishCompleteReason, PublishPacket, PublishReceivedPacket, PublishReleasePacket,
        PublishReleaseReason, QoS, SubscribeAckPacket, SubscribeAckReason, SubscribePacket,
        UnsubscribeAckPacket, UnsubscribeAckReason, UnsubscribePacket,
    },
};
use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time,
};

/// Client connected but not yet authenticated.
struct UnauthenticatedSession {
    connect_packet: ConnectPacket,
    client_sender: Sender<ClientMessage>,
}

impl UnauthenticatedSession {
    /// Construct a new UnauthenticatedSession.
    fn new(connect_packet: ConnectPacket, client_sender: Sender<ClientMessage>) -> Self {
        Self { connect_packet, client_sender }
    }

    /// Send a `ClientMessage` to the client via the channel handle.
    /// This is a fire and forget operation because upon any error on the
    /// connection the `Client` will send a `BrokerMessage::Disconnect`.
    async fn send(&self, message: ClientMessage) {
        drop(self.client_sender.send(message).await);
    }
}

#[derive(Debug)]
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

/// Unique identifier for a connection
pub type ConnectionId = u64;

/// Client ID
pub type ClientId = String;

#[derive(Debug)]
pub enum BrokerMessage {
    Connect(ConnectionId, Box<ConnectPacket>, Sender<ClientMessage>),
    Disconnect(ConnectionId, String, WillDisconnectLogic),
    Authenticate(ConnectionId, ClientId, AuthenticatePacket),
    Publish(ConnectionId, ClientId, Box<PublishPacket>),
    PublishAck(ConnectionId, ClientId, PublishAckPacket), // TODO - This can be handled by the client task
    PublishRelease(ConnectionId, ClientId, PublishReleasePacket), // TODO - This can be handled by the client task
    PublishReceived(ConnectionId, ClientId, PublishReceivedPacket),
    PublishComplete(ConnectionId, ClientId, PublishCompletePacket),
    PublishFinalWill(ConnectionId, ClientId, FinalWill),
    Subscribe(ConnectionId, ClientId, SubscribePacket), // TODO - replace string client_id with int
    Unsubscribe(ConnectionId, ClientId, UnsubscribePacket), // TODO - replace string client_id with int
}

pub struct Broker<A = Noop> {
    /// A map of client IDs to unauthenticated sessions. Once a client passes
    /// authentication, the session exended is moved to the `session` map.
    unauthenticated_sessions: HashMap<ConnectionId, UnauthenticatedSession>,
    sessions: HashMap<String, Session>,
    sender: Sender<BrokerMessage>,
    receiver: Receiver<BrokerMessage>,
    subscriptions: SubscriptionTree<SessionSubscription>,
    #[allow(unused)]
    plugin: A,
}

impl Default for Broker {
    fn default() -> Self {
        Broker::<Noop>::new()
    }
}

impl<A: Plugin> Broker<A> {
    /// Construct a new Broker.
    pub fn new() -> Broker {
        let (sender, receiver) = mpsc::channel(100);

        Broker {
            unauthenticated_sessions: HashMap::new(),
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
            unauthenticated_sessions: HashMap::new(),
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
            None
        } else {
            existing_session
        }
    }

    async fn handle_new_client(
        &mut self,
        connection_id: ConnectionId,
        connect_packet: ConnectPacket,
        client_msg_sender: Sender<ClientMessage>,
    ) {
        debug!(
            "Trying to authenticate client {} (connection {})",
            connect_packet.client_id, connection_id
        );
        match self.plugin.on_connect(&connect_packet) {
            AuthentificationResult::Reason(ConnectReason::Success) => {
                info!("Authentification successful for client {}", connect_packet.client_id);
                self.handle_authenticated_client(connect_packet, client_msg_sender).await;
            },
            AuthentificationResult::Reason(reason_code) => {
                info!(
                    "Authentification reason code for client {} is {:?}",
                    connect_packet.client_id, reason_code
                );
                let connect_ack = ConnectAckPacket {
                    session_present: false,
                    reason_code,
                    session_expiry_interval: None,
                    receive_maximum: None,
                    maximum_qos: None,
                    retain_available: None,
                    maximum_packet_size: None,
                    assigned_client_identifier: None,
                    topic_alias_maximum: None,
                    reason_string: None,
                    user_properties: Vec::with_capacity(0),
                    wildcard_subscription_available: None,
                    subscription_identifiers_available: None,
                    shared_subscription_available: None,
                    server_keep_alive: None,
                    response_information: None,
                    server_reference: None,
                    authentication_method: None,
                    authentication_data: None,
                };

                // Send a disconnect packet to the client. Ignore send errors because
                // the client could already be disconnected and the rx handle of this
                // channel is dropped.
                debug!(
                    "Sending CONNACK to client ID {} with reason code {:?}",
                    connect_packet.client_id, reason_code
                );
                client_msg_sender
                    .send(ClientMessage::Packet(Packet::ConnectAck(connect_ack)))
                    .await
                    .ok();

                debug!(
                    "Sending DISCONNECT to client {} with disconnect reason code {:?}",
                    connect_packet.client_id,
                    DisconnectReason::NotAuthorized
                );
                client_msg_sender
                    .send(ClientMessage::Disconnect(DisconnectReason::NotAuthorized))
                    .await
                    .ok();
            },
            AuthentificationResult::Packet(packet) => {
                client_msg_sender
                    .send(ClientMessage::Packet(Packet::Authenticate(packet)))
                    .await
                    .ok();
                let client_id = connect_packet.client_id.clone();
                info!("Adding unauthenticated session for client ID {}", client_id);
                let unauthenticated_session =
                    UnauthenticatedSession::new(connect_packet, client_msg_sender);
                self.unauthenticated_sessions.insert(connection_id, unauthenticated_session);
            },
        }
    }

    async fn handle_authenticated_client(
        &mut self,
        connect_packet: ConnectPacket,
        client_msg_sender: Sender<ClientMessage>,
    ) {
        let mut takeover_session = self
            .take_over_existing_client(&connect_packet.client_id, connect_packet.clean_start)
            .await;
        let session_present = takeover_session.is_some();

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
        let session_expiry_duration = session_expiry_interval.map(|i| {
            let duration = Duration::from_secs(i.0 as u64);
            debug!(
                "Client ID {} Session expiry interval is {:?}",
                connect_packet.client_id, duration
            );
            duration
        });

        // The user provided plugin decided when processing the Connect packet that this
        // client is authenticated. This could be the case for no authentication needed or
        // a username password authentication. The broker shall not wait for addition
        // `Authentification` packets from the client before the client is in the authenticated
        // state.
        // If the client is not authenticated the `ConnectAckPacket` is sent once authentification
        // succeeds or fails.
        // Send conack if the auth is already successful and complete
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
            user_properties: Vec::with_capacity(0),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };

        // If the client disconnected in the meantime, the rx part of the client handle is dropped
        // and a send attempt will fail. Ignore this error, because the disconnection is handled
        // by a BrokerMessage::Disconnect.
        client_msg_sender.send(ClientMessage::Packet(Packet::ConnectAck(connect_ack))).await.ok();

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

    /// Handle authenticate packets. Query the plugin and send a `ConnectAckPacket` or `Authenticate`
    /// packet if needed. If the plugin authenticates the client (session) proceed with the session
    /// setup etc. and remove the unauthenticated session.
    async fn handle_authenticate(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: AuthenticatePacket,
    ) {
        debug!("Trying to authenticate client {} (connection {})", client_id, connection_id);

        let entry = match self.unauthenticated_sessions.entry(connection_id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(entry) => {
                warn!("Received authenticate packet for unknown client ID {}", entry.key());
                return;
            },
        };

        match self.plugin.on_authenticate(&packet) {
            AuthentificationResult::Reason(ConnectReason::Success) => {
                let (client_id, UnauthenticatedSession { client_sender, connect_packet }) =
                    entry.remove_entry();
                info!("Authentification successful for client ID {}", client_id);
                self.handle_authenticated_client(connect_packet, client_sender).await;
            },
            AuthentificationResult::Reason(reason_code) => {
                info!("Authentification result for client ID {} is {:?}", entry.key(), reason_code);
                let connect_ack = ConnectAckPacket {
                    // Variable header
                    session_present: false,
                    reason_code,

                    // Properties
                    session_expiry_interval: None,
                    receive_maximum: None,
                    maximum_qos: None,
                    retain_available: None,
                    maximum_packet_size: None,
                    assigned_client_identifier: None,
                    topic_alias_maximum: None,
                    reason_string: None,
                    user_properties: Vec::with_capacity(0),
                    wildcard_subscription_available: None,
                    subscription_identifiers_available: None,
                    shared_subscription_available: None,
                    server_keep_alive: None,
                    response_information: None,
                    server_reference: None,
                    authentication_method: None,
                    authentication_data: None,
                };

                // If the client disconnected in the meantime, the rx part of the client handle is dropped
                // and a send attempt will fail. Ignore this error, because the disconnection is handled
                // by a BrokerMessage::Disconnect.
                let session = entry.get();
                session.send(ClientMessage::Packet(Packet::ConnectAck(connect_ack))).await;
                session.send(ClientMessage::Disconnect(DisconnectReason::NotAuthorized)).await;
            },
            AuthentificationResult::Packet(packet) => {
                let session = entry.get();
                session.send(ClientMessage::Packet(Packet::Authenticate(packet))).await;
            },
        }
    }

    async fn handle_subscribe(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: SubscribePacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!(
                "Ignoring subscribe packet from unauthenticated client ID {} on connection {}",
                client_id, connection_id
            );
            return;
        }

        let subscriptions = &mut self.subscriptions;

        if let Some(session) = self.sessions.get_mut(&client_id) {
            let plugin_ack = self.plugin.on_subscribe(&packet);

            // If a Server receives a SUBSCRIBE packet containing a Topic Filter that
            // is identical to a Non‑shared Subscription’s Topic Filter for the current
            // Session, then it MUST replace that existing Subscription with a new Subscription.
            for (topic, plugin_code) in
                packet.subscription_topics.iter().zip(&plugin_ack.reason_codes)
            {
                // Check only subscriptions that the plugin didn't reject.
                match plugin_code {
                    SubscribeAckReason::GrantedQoSZero
                    | SubscribeAckReason::GrantedQoSOne
                    | SubscribeAckReason::GrantedQoSTwo => (),
                    _ => continue,
                }

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
                .zip(plugin_ack.reason_codes)
                .map(|(topic, plugin_reason)| match plugin_reason {
                    SubscribeAckReason::GrantedQoSZero
                    | SubscribeAckReason::GrantedQoSOne
                    | SubscribeAckReason::GrantedQoSTwo => {
                        let session_subscription = SessionSubscription {
                            client_id: client_id.clone(),
                            maximum_qos: topic.maximum_qos,
                        };
                        let token = subscriptions.insert(&topic.topic_filter, session_subscription);

                        session.subscription_tokens.push((topic.topic_filter.clone(), token));
                        plugin_reason
                    },
                    reason => reason,
                })
                .collect();

            let subscribe_ack = SubscribeAckPacket {
                packet_id: packet.packet_id,
                reason_string: plugin_ack.reason_string,
                user_properties: plugin_ack.user_properties,
                reason_codes: granted_qos_values,
            };

            session.send(ClientMessage::Packet(Packet::SubscribeAck(subscribe_ack))).await;
        }
    }

    async fn handle_unsubscribe(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: UnsubscribePacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!(
                "Ignoring unsubscribe packet from unauthenticated client ID {} on connection {}",
                client_id, connection_id
            );
            return;
        }

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

    fn handle_disconnect(
        &mut self,
        connection_id: ConnectionId,
        client_id: String,
        will_disconnect_logic: WillDisconnectLogic,
    ) {
        if self.unauthenticated_sessions.remove(&connection_id).is_some() {
            info!("Removing unauthenticated session for client ID {}", client_id);
            return;
        }

        info!("Client ID {} disconnected", client_id);

        self.plugin.on_disconnect(&client_id);

        let mut disconnect_will = None;
        let mut session_expiry_duration = None;

        if let Entry::Occupied(mut session_entry) = self.sessions.entry(client_id.clone()) {
            {
                let session = session_entry.get_mut();
                session.client_sender.take();
            }

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
                            time::sleep(will_send_delay_duration).await;
                            broker_sender
                                .send(BrokerMessage::PublishFinalWill(
                                    connection_id,
                                    client_id,
                                    will,
                                ))
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

    async fn handle_publish(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: PublishPacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!(
                "Discarding publish packet from unauthenticated client ID {} on connection {}",
                client_id, connection_id
            );
            return;
        }

        if let Some(session) = self.sessions.get_mut(&client_id) {
            match packet.qos {
                QoS::AtMostOnce => {
                    if self.plugin.on_publish_received_qos0(&packet) {
                        self.publish_message(packet).await;
                    }
                },
                QoS::AtLeastOnce => {
                    let (publish, publish_ack) = self.plugin.on_publish_received_qos1(&packet);
                    if let Some(publish_ack) = publish_ack {
                        session.send(ClientMessage::Packet(Packet::PublishAck(publish_ack))).await;
                    }
                    if publish {
                        self.publish_message(packet).await;
                    }
                },
                // For QoS2, ensure this packet isn't delivered twice. So if we have an outgoing
                // publish receive with the same ID, just send the publish receive again but don't forward
                // the message.
                QoS::ExactlyOnce => {
                    let (mut publish, publish_rec) = self.plugin.on_publish_received_qos2(&packet);

                    if let Some(publish_recv) = publish_rec {
                        let packet_id = publish_recv.packet_id;
                        let is_dup = session.outgoing_publish_receives.contains(&packet_id);

                        publish = publish && !is_dup;

                        if !is_dup {
                            session.outgoing_publish_receives.push(packet_id)
                        }

                        session
                            .send(ClientMessage::Packet(Packet::PublishReceived(publish_recv)))
                            .await;
                    }

                    if publish {
                        self.publish_message(packet).await;
                    }
                },
            }
        }
    }

    fn handle_publish_ack(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: PublishAckPacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!(
                "Discarding publish ack packet from unauthenticated client ID {} on connection {}",
                client_id, connection_id
            );
            return;
        }

        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.remove_outgoing_publish(packet.packet_id);
        }
    }

    async fn handle_publish_release(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: PublishReleasePacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!("Discarding publish release packet from unauthenticated client ID {} on connection {}", client_id, connection_id);
            return;
        }

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

    async fn handle_publish_received(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: PublishReceivedPacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!("Discarding publish received packet from unauthenticated client ID {} on connection {}", client_id, connection_id);
            return;
        }

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

    fn handle_publish_complete(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        packet: PublishCompletePacket,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!("Discarding publish complete packet from unauthenticated client ID {} on connection {}", client_id, connection_id);
            return;
        }

        if let Some(session) = self.sessions.get_mut(&client_id) {
            if let Some(pos) =
                session.outgoing_publish_released.iter().position(|x| *x == packet.packet_id)
            {
                session.outgoing_publish_released.remove(pos);
            }
        }
    }

    async fn publish_final_will(
        &mut self,
        connection_id: ConnectionId,
        client_id: ClientId,
        final_will: FinalWill,
    ) {
        if self.unauthenticated_sessions.contains_key(&connection_id) {
            warn!(
                "Discarding final will packet from unauthenticated client ID {} on connection {}",
                client_id, connection_id
            );
            return;
        }

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
                BrokerMessage::Connect(connection_id, connect_packet, client_msg_sender) => {
                    self.handle_new_client(connection_id, *connect_packet, client_msg_sender).await;
                },
                BrokerMessage::Disconnect(connection_id, client_id, will_disconnect_logic) => {
                    self.handle_disconnect(connection_id, client_id, will_disconnect_logic);
                },
                BrokerMessage::Authenticate(connection_id, client_id, packet) => {
                    self.handle_authenticate(connection_id, client_id, packet).await;
                },
                BrokerMessage::Subscribe(connection_id, client_id, packet) => {
                    self.handle_subscribe(connection_id, client_id, packet).await;
                },
                BrokerMessage::Unsubscribe(connection_id, client_id, packet) => {
                    self.handle_unsubscribe(connection_id, client_id, packet).await;
                },
                BrokerMessage::Publish(connection_id, client_id, packet) => {
                    self.handle_publish(connection_id, client_id, *packet).await;
                },
                BrokerMessage::PublishAck(connection_id, client_id, packet) => {
                    self.handle_publish_ack(connection_id, client_id, packet);
                },
                BrokerMessage::PublishRelease(connection_id, client_id, packet) => {
                    self.handle_publish_release(connection_id, client_id, packet).await;
                },
                BrokerMessage::PublishReceived(connection_id, client_id, packet) => {
                    self.handle_publish_received(connection_id, client_id, packet).await;
                },
                BrokerMessage::PublishComplete(connection_id, client_id, packet) => {
                    self.handle_publish_complete(connection_id, client_id, packet);
                },
                BrokerMessage::PublishFinalWill(connection_id, client_id, final_will) => {
                    self.publish_final_will(connection_id, client_id, final_will).await;
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
            user_name: Some("test".into()),
            password: Some("test".into()),
        };

        let _ = broker_tx
            .send(BrokerMessage::Connect(0, Box::new(connect_packet), sender))
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
                0,
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
