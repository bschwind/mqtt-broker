use crate::types::{Packet, SubscriptionTopic};
use std::collections::HashSet;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Session {
    pub subscriptions: HashSet<SubscriptionTopic>,
    pub shared_subscriptions: HashSet<SubscriptionTopic>,
}

pub struct Broker {
    sessions: Vec<Session>,
    sender: Sender<Packet>,
    receiver: Receiver<Packet>,
}

impl Broker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);

        Self { sessions: vec![], sender, receiver }
    }

    pub fn sender(&self) -> Sender<Packet> {
        self.sender.clone()
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            println!("broker got a message: {:?}", msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{broker::Broker, types::Packet};

    #[test]
    fn do_stuff() {
        let broker = Broker::new();
        let sender = broker.sender();

        println!("hey");
    }
}
