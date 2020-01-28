use crate::client::ClientMessage;
use mqtt_v5::types::{
    properties::AssignedClientIdentifier, ConnectAckPacket, ConnectReason, ProtocolVersion,
    SubscribeAckPacket, SubscribeAckReason, SubscribePacket, SubscriptionTopic,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Session {
    pub protocol_version: ProtocolVersion,
    pub subscriptions: HashSet<SubscriptionTopic>,
    pub shared_subscriptions: HashSet<SubscriptionTopic>,
    pub client_sender: Sender<ClientMessage>,
}

impl Session {
    pub fn new(protocol_version: ProtocolVersion, client_sender: Sender<ClientMessage>) -> Self {
        Self {
            protocol_version,
            subscriptions: HashSet::new(),
            shared_subscriptions: HashSet::new(),
            client_sender,
        }
    }
}

#[derive(Debug)]
pub enum BrokerMessage {
    NewClient(String, ProtocolVersion, Sender<ClientMessage>),
    Publish,
    Subscribe(String, SubscribePacket), // TODO - replace string client_id with int
    Disconnect(String),
}

pub struct Broker {
    sessions: HashMap<String, Session>,
    sender: Sender<BrokerMessage>,
    receiver: Receiver<BrokerMessage>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);

        Self { sessions: HashMap::new(), sender, receiver }
    }

    pub fn sender(&self) -> Sender<BrokerMessage> {
        self.sender.clone()
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                BrokerMessage::NewClient(client_id, protocol_version, mut client_msg_sender) => {
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
                        assigned_client_identifier: Some(AssignedClientIdentifier(
                            client_id.clone(),
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

                    let _ = client_msg_sender.try_send(ClientMessage::ConnectAck(connect_ack));

                    self.sessions
                        .insert(client_id, Session::new(protocol_version, client_msg_sender));
                },
                BrokerMessage::Subscribe(client_id, packet) => {
                    if let Some(session) = self.sessions.get_mut(&client_id) {
                        // TODO - actually add subscription
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
                            .try_send(ClientMessage::SubscribeAck(subscribe_ack));
                    }
                },
                BrokerMessage::Disconnect(client_id) => {
                    println!("Client ID {} disconnected", client_id);
                    self.sessions.remove(&client_id);
                },
                x => {
                    println!("broker got a message: {:?}", x);
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::broker::Broker;

    #[test]
    fn do_stuff() {
        let broker = Broker::new();
        let sender = broker.sender();

        println!("hey");
    }
}
