use crate::{client::ClientMessage, tree::SubscriptionTree};
use mqtt_v5::{
    topic::TopicFilter,
    types::{
        properties::AssignedClientIdentifier, ConnectAckPacket, ConnectReason, Packet,
        ProtocolVersion, PublishAckPacket, PublishAckReason, PublishPacket, QoS,
        SubscribeAckPacket, SubscribeAckReason, SubscribePacket,
    },
};
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Session {
    pub protocol_version: ProtocolVersion,
    // pub subscriptions: HashSet<SubscriptionTopic>,
    // pub shared_subscriptions: HashSet<SubscriptionTopic>,
    pub client_sender: Sender<ClientMessage>,

    // Used to unsubscribe from topics
    subscription_tokens: Vec<(TopicFilter, u64)>,

    // Keep track of outgoing packets with QoS 1 or 2
    outgoing_packets: Vec<PublishPacket>,

    packet_counter: u16,
}

impl Session {
    pub fn new(protocol_version: ProtocolVersion, client_sender: Sender<ClientMessage>) -> Self {
        Self {
            protocol_version,
            // subscriptions: HashSet::new(),
            // shared_subscriptions: HashSet::new(),
            client_sender,
            subscription_tokens: Vec::new(),
            outgoing_packets: Vec::new(),
            packet_counter: 1,
        }
    }

    pub fn store_outgoing_publish(&mut self, mut publish: PublishPacket) -> u16 {
        // TODO(bschwind) - Prevent using packet IDs which already exist in `outgoing_packets`
        let packet_id = self.packet_counter;
        publish.packet_id = Some(self.packet_counter);
        publish.is_duplicate = false;
        self.packet_counter += 1;

        // Handle u16 wraparound, 0 is an invalid packet ID
        if self.packet_counter == 0 {
            self.packet_counter += 1;
        }

        self.outgoing_packets.push(publish);

        packet_id
    }

    pub fn remove_outgoing_publish(&mut self, packet_id: u16) {
        if let Some(pos) = self.outgoing_packets.iter().position(|p| p.packet_id == Some(packet_id))
        {
            self.outgoing_packets.remove(pos);
        }
    }
}

#[derive(Debug)]
struct SessionSubscription {
    client_id: String,
    maximum_qos: QoS,
}

#[derive(Debug)]
pub enum BrokerMessage {
    NewClient(String, ProtocolVersion, Sender<ClientMessage>),
    Publish(String, PublishPacket),
    PublishAck(String, PublishAckPacket), // TODO - This can be handled by the client task
    Subscribe(String, SubscribePacket),   // TODO - replace string client_id with int
    Disconnect(String),
}

pub struct Broker {
    sessions: HashMap<String, Session>,
    sender: Sender<BrokerMessage>,
    receiver: Receiver<BrokerMessage>,
    subscriptions: SubscriptionTree<SessionSubscription>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);

        Self { sessions: HashMap::new(), sender, receiver, subscriptions: SubscriptionTree::new() }
    }

    pub fn sender(&self) -> Sender<BrokerMessage> {
        self.sender.clone()
    }

    fn handle_new_client(
        &mut self,
        client_id: String,
        protocol_version: ProtocolVersion,
        mut client_msg_sender: Sender<ClientMessage>,
    ) {
        let mut session_present = false;

        if let Some(mut session) = self.sessions.remove(&client_id) {
            // Tell session to disconnect
            session_present = true;
            println!("Telling existing session to disconnect");

            // TODO(bschwind) - Unify this logic with handle_disconnect()
            for (topic, token) in session.subscription_tokens {
                self.subscriptions.remove(&topic, token);
            }

            // TODO(bschwind) - Publish the client's will and send a Disconnect
            //                  packet with Reason Code 0x8E (Session taken over).
            //                  Also remove all session subscriptions like handle_disconnect().

            let _ = session.client_sender.try_send(ClientMessage::Disconnect);
        }

        println!("Client ID {} connected (Version: {:?})", client_id, protocol_version);

        let connect_ack = ConnectAckPacket {
            // Variable header
            session_present,
            reason_code: ConnectReason::Success,

            // Properties
            session_expiry_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            assigned_client_identifier: Some(AssignedClientIdentifier(client_id.clone())),
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

        let _ = client_msg_sender.try_send(ClientMessage::Packet(Packet::ConnectAck(connect_ack)));

        self.sessions.insert(client_id, Session::new(protocol_version, client_msg_sender));
    }

    fn handle_subscribe(&mut self, client_id: String, packet: SubscribePacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            let subscriptions = &mut self.subscriptions;

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

            let _ = session
                .client_sender
                .try_send(ClientMessage::Packet(Packet::SubscribeAck(subscribe_ack)));
        }
    }

    fn handle_disconnect(&mut self, client_id: String) {
        println!("Client ID {} disconnected", client_id);
        if let Some(session) = self.sessions.remove(&client_id) {
            for (topic, token) in session.subscription_tokens {
                self.subscriptions.remove(&topic, token);
            }
        }
    }

    fn handle_publish(&mut self, client_id: String, packet: PublishPacket) {
        let sessions = &mut self.sessions;
        let topic = &packet.topic;
        self.subscriptions.matching_subscribers(topic, |session_subscription| {
            if let Some(session) = sessions.get_mut(&session_subscription.client_id) {
                let outgoing_packet_id = match session_subscription.maximum_qos {
                    QoS::AtLeastOnce | QoS::ExactlyOnce => {
                        Some(session.store_outgoing_publish(packet.clone()))
                    },
                    _ => None,
                };

                let outgoing_packet = PublishPacket {
                    packet_id: outgoing_packet_id,
                    is_duplicate: false,
                    ..packet.clone()
                };

                let _ = session
                    .client_sender
                    .try_send(ClientMessage::Packet(Packet::Publish(outgoing_packet)));
            }
        });

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

                    let _ = session
                        .client_sender
                        .try_send(ClientMessage::Packet(Packet::PublishAck(publish_ack)));
                }
            },
            QoS::ExactlyOnce => {},
        }
    }

    fn handle_publish_ack(&mut self, client_id: String, packet: PublishAckPacket) {
        if let Some(session) = self.sessions.get_mut(&client_id) {
            session.remove_outgoing_publish(packet.packet_id);
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                BrokerMessage::NewClient(client_id, protocol_version, client_msg_sender) => {
                    self.handle_new_client(client_id, protocol_version, client_msg_sender);
                },
                BrokerMessage::Subscribe(client_id, packet) => {
                    self.handle_subscribe(client_id, packet);
                },
                BrokerMessage::Disconnect(client_id) => {
                    self.handle_disconnect(client_id);
                },
                BrokerMessage::Publish(client_id, packet) => {
                    self.handle_publish(client_id, packet);
                },
                BrokerMessage::PublishAck(client_id, packet) => {
                    self.handle_publish_ack(client_id, packet);
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
    };
    use mqtt_v5::types::{properties::*, ProtocolVersion, *};
    use tokio::{
        runtime::Runtime,
        sync::mpsc::{self, Sender},
    };

    async fn run_client(mut broker_tx: Sender<BrokerMessage>) {
        let (sender, mut receiver) = mpsc::channel(5);
        let _ = broker_tx
            .send(BrokerMessage::NewClient("TEST".to_string(), ProtocolVersion::V500, sender))
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
        let broker = Broker::new();
        let sender = broker.sender();

        let mut runtime = Runtime::new().unwrap();

        runtime.spawn(broker.run());
        runtime.block_on(run_client(sender));
    }
}
