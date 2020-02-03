use crate::{client::ClientMessage, tree::SubscriptionTree};
use mqtt_v5::{
    topic::TopicFilter,
    types::{
        properties::AssignedClientIdentifier, ConnectAckPacket, ConnectReason, Packet,
        ProtocolVersion, PublishPacket, SubscribeAckPacket, SubscribeAckReason, SubscribePacket,
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
}

impl Session {
    pub fn new(protocol_version: ProtocolVersion, client_sender: Sender<ClientMessage>) -> Self {
        Self {
            protocol_version,
            // subscriptions: HashSet::new(),
            // shared_subscriptions: HashSet::new(),
            client_sender,
            subscription_tokens: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub enum BrokerMessage {
    NewClient(String, ProtocolVersion, Sender<ClientMessage>),
    Publish(String, PublishPacket),
    Subscribe(String, SubscribePacket), // TODO - replace string client_id with int
    Disconnect(String),
}

pub struct Broker {
    sessions: HashMap<String, Session>,
    sender: Sender<BrokerMessage>,
    receiver: Receiver<BrokerMessage>,
    subscriptions: SubscriptionTree<String>,
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
            for topic in &packet.subscription_topics {
                let token = self.subscriptions.insert(&topic.topic_filter, client_id.clone());
                session.subscription_tokens.push((topic.topic_filter.clone(), token));
            }

            let subscribe_ack = SubscribeAckPacket {
                packet_id: packet.packet_id,
                reason_string: None,
                user_properties: vec![],
                reason_codes: packet
                    .subscription_topics
                    .iter()
                    .map(|_| SubscribeAckReason::GrantedQoSOne)
                    .collect(),
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

    fn handle_publish(&mut self, _client_id: String, packet: PublishPacket) {
        // TODO - Ideall we shouldn't allocate here
        let mut clients = vec![];
        self.subscriptions.matching_subscribers(&packet.topic, |client_id| {
            clients.push(client_id.clone());
        });

        for client_id in clients {
            if let Some(session) = self.sessions.get_mut(&client_id) {
                let _ = session
                    .client_sender
                    .try_send(ClientMessage::Packet(Packet::Publish(packet.clone())));
            }
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
                reason_codes: vec![SubscribeAckReason::GrantedQoSOne,],
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
