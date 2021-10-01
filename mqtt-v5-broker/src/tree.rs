use mqtt_v5::topic::{Topic, TopicFilter, TopicLevel};
use std::collections::{hash_map::Entry, HashMap};

// TODO(bschwind) - Support shared subscriptions

#[derive(Debug)]
pub struct SubscriptionTreeNode<T> {
    subscribers: Vec<(u64, T)>,
    single_level_wildcards: Option<Box<SubscriptionTreeNode<T>>>,
    multi_level_wildcards: Vec<(u64, T)>,
    concrete_topic_levels: HashMap<String, SubscriptionTreeNode<T>>,
}

#[derive(Debug)]
pub struct SubscriptionTree<T> {
    root: SubscriptionTreeNode<T>,
    counter: u64,
}

impl<T: std::fmt::Debug> SubscriptionTree<T> {
    pub fn new() -> Self {
        Self { root: SubscriptionTreeNode::new(), counter: 0 }
    }

    pub fn insert(&mut self, topic_filter: &TopicFilter, value: T) -> u64 {
        let counter = self.counter;
        self.root.insert(topic_filter, value, counter);
        self.counter += 1;

        counter
    }

    pub fn matching_subscribers(&self, topic: &Topic) -> impl Iterator<Item = &T> {
        self.root.matching_subscribers(topic)
    }

    pub fn remove(&mut self, topic_filter: &TopicFilter, counter: u64) -> Option<T> {
        self.root.remove(topic_filter, counter)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.root.is_empty()
    }
}

impl<T: std::fmt::Debug> SubscriptionTreeNode<T> {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            single_level_wildcards: None,
            multi_level_wildcards: Vec::new(),
            concrete_topic_levels: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.single_level_wildcards.is_none()
            && self.multi_level_wildcards.is_empty()
            && self.concrete_topic_levels.is_empty()
    }

    fn insert(&mut self, topic_filter: &TopicFilter, value: T, counter: u64) {
        let mut current_tree = self;
        let mut multi_level = false;

        for level in topic_filter.levels() {
            match level {
                TopicLevel::SingleLevelWildcard => {
                    if current_tree.single_level_wildcards.is_none() {
                        current_tree.single_level_wildcards =
                            Some(Box::new(SubscriptionTreeNode::new()));
                    }

                    current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                },
                TopicLevel::MultiLevelWildcard => {
                    multi_level = true;
                    break;
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if !current_tree.concrete_topic_levels.contains_key(concrete_topic_level) {
                        current_tree
                            .concrete_topic_levels
                            .insert(concrete_topic_level.to_string(), SubscriptionTreeNode::new());
                    }

                    // TODO - Do this without another hash lookup
                    current_tree =
                        current_tree.concrete_topic_levels.get_mut(concrete_topic_level).unwrap();
                },
            }
        }

        if multi_level {
            current_tree.multi_level_wildcards.push((counter, value));
        } else {
            current_tree.subscribers.push((counter, value));
        }
    }

    fn remove(&mut self, topic_filter: &TopicFilter, counter: u64) -> Option<T> {
        let mut current_tree = self;
        let mut stack: Vec<(*mut SubscriptionTreeNode<T>, usize)> = vec![];

        let levels: Vec<TopicLevel> = topic_filter.levels().collect();
        let mut level_index = 0;

        for level in &levels {
            match level {
                TopicLevel::SingleLevelWildcard => {
                    if current_tree.single_level_wildcards.is_some() {
                        stack.push((&mut *current_tree, level_index));
                        level_index += 1;

                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    } else {
                        return None;
                    }
                },
                TopicLevel::MultiLevelWildcard => {
                    break;
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if current_tree.concrete_topic_levels.contains_key(*concrete_topic_level) {
                        stack.push((&mut *current_tree, level_index));
                        level_index += 1;

                        current_tree = current_tree
                            .concrete_topic_levels
                            .get_mut(*concrete_topic_level)
                            .unwrap();
                    } else {
                        return None;
                    }
                },
            }
        }

        // Get the return value
        let return_val = {
            let level = &levels[levels.len() - 1];

            if *level == TopicLevel::MultiLevelWildcard {
                if let Some(pos) =
                    current_tree.multi_level_wildcards.iter().position(|(c, _)| *c == counter)
                {
                    Some(current_tree.multi_level_wildcards.remove(pos))
                } else {
                    None
                }
            } else if let Some(pos) =
                current_tree.subscribers.iter().position(|(c, _)| *c == counter)
            {
                Some(current_tree.subscribers.remove(pos))
            } else {
                None
            }
        };

        // Go up the stack, cleaning up empty nodes
        while let Some((stack_val, level_index)) = stack.pop() {
            let mut tree = unsafe { &mut *stack_val };

            let level = &levels[level_index];

            match level {
                TopicLevel::SingleLevelWildcard => {
                    if tree.single_level_wildcards.as_ref().map(|t| t.is_empty()).unwrap_or(false) {
                        tree.single_level_wildcards = None;
                    }
                },
                TopicLevel::MultiLevelWildcard => {
                    // TODO - Ignore this case?
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if let Entry::Occupied(o) =
                        tree.concrete_topic_levels.entry((*concrete_topic_level).to_string())
                    {
                        if o.get().is_empty() {
                            o.remove_entry();
                        }
                    }
                },
            }
        }

        return_val.map(|(_, val)| val)
    }

    fn matching_subscribers(&self, topic: &Topic) -> impl Iterator<Item = &T> {
        let mut subscriptions = Vec::new();
        let mut tree_stack = vec![(self, 0)];
        let levels: Vec<TopicLevel> = topic.levels().collect();

        while !tree_stack.is_empty() {
            let (current_tree, current_level) = tree_stack.pop().unwrap();
            let level = &levels[current_level];

            // Don't allow wildcard subscribers to receive messages
            // with leading dollar signs, like '$SYS/stats'
            if current_level != 0 || !level.has_leading_dollar() {
                subscriptions.extend(
                    current_tree.multi_level_wildcards.iter().map(|(_, subscriber)| subscriber),
                );
            }

            if let Some(sub_tree) = &current_tree.single_level_wildcards {
                // Don't allow wildcard subscribers to receive messages
                // with leading dollar signs, like '$SYS/stats'
                if current_level != 0 || !level.has_leading_dollar() {
                    if current_level + 1 < levels.len() {
                        tree_stack.push((sub_tree, current_level + 1));
                    } else {
                        subscriptions
                            .extend(sub_tree.subscribers.iter().map(|(_, subscriber)| subscriber));
                    }
                }
            }

            if let TopicLevel::Concrete(level) = level {
                if current_tree.concrete_topic_levels.contains_key(*level) {
                    let sub_tree = current_tree.concrete_topic_levels.get(*level).unwrap();

                    if current_level + 1 < levels.len() {
                        let sub_tree = current_tree.concrete_topic_levels.get(*level).unwrap();
                        tree_stack.push((sub_tree, current_level + 1));
                    } else {
                        subscriptions
                            .extend(sub_tree.subscribers.iter().map(|(_, subscriber)| subscriber));

                        // TODO(bschwind) - Verify this works properly with better tests.
                        subscriptions.extend(
                            sub_tree.multi_level_wildcards.iter().map(|(_, subscriber)| subscriber),
                        );
                    }
                }
            }
        }
        subscriptions.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::tree::SubscriptionTree;
    use std::{collections::HashSet, fmt::Debug, hash::Hash};

    fn assert_subscribers<T: Debug + Hash + Eq + Clone>(
        tree: &SubscriptionTree<T>,
        topic: &str,
        subscribers: &[T],
    ) {
        let expected_set = subscribers.iter().cloned().collect::<HashSet<_>>();
        let mut actual_set = HashSet::new();

        for subscriber in tree.matching_subscribers(&topic.parse().unwrap()) {
            actual_set.insert(subscriber.clone());
        }

        assert_eq!(expected_set, actual_set);
    }

    #[test]
    fn test_insert() {
        let mut sub_tree = SubscriptionTree::new();
        sub_tree.insert(&"home/kitchen/temperature".parse().unwrap(), 1);
        sub_tree.insert(&"home/kitchen/humidity".parse().unwrap(), 2);
        sub_tree.insert(&"home/kitchen".parse().unwrap(), 3);
        sub_tree.insert(&"home/+/humidity".parse().unwrap(), 4);
        sub_tree.insert(&"home/+".parse().unwrap(), 5);
        sub_tree.insert(&"home/#".parse().unwrap(), 6);
        sub_tree.insert(&"home/+/temperature".parse().unwrap(), 7);
        sub_tree.insert(&"office/stairwell/temperature".parse().unwrap(), 8);
        sub_tree.insert(&"office/+/+".parse().unwrap(), 9);
        sub_tree.insert(&"office/+/+/some_desk/+/fan_speed/+/temperature".parse().unwrap(), 10);
        sub_tree.insert(&"office/+/+/some_desk/+/#".parse().unwrap(), 11);
        sub_tree.insert(&"sport/tennis/+".parse().unwrap(), 21);
        sub_tree.insert(&"#".parse().unwrap(), 12);

        assert_subscribers(&sub_tree, "home", &[6, 12]);
        assert_subscribers(&sub_tree, "home/kitchen", &[3, 5, 6, 12]);
        assert_subscribers(&sub_tree, "home/kitchen/humidity", &[2, 4, 6, 12]);
        assert_subscribers(&sub_tree, "office/stairwell/temperature", &[8, 9, 12]);
        assert_subscribers(
            &sub_tree,
            "office/tokyo/shibuya/some_desk/cpu_1/fan_speed/blade_4/temperature",
            &[10, 11, 12],
        );
        assert_subscribers(&sub_tree, "sport/tennis/player1", &[21, 12]);
        assert_subscribers(&sub_tree, "sport/tennis/player2", &[21, 12]);
        assert_subscribers(&sub_tree, "sport/tennis/player1/ranking", &[12]);
    }

    #[test]
    fn test_remove() {
        let mut sub_tree = SubscriptionTree::new();
        let sub_1 = sub_tree.insert(&"home/kitchen/temperature".parse().unwrap(), "sub_1");
        let sub_2 = sub_tree.insert(&"home/kitchen/temperature".parse().unwrap(), "sub_2");
        let sub_3 = sub_tree.insert(&"home/kitchen/humidity".parse().unwrap(), "sub_3");
        let sub_4 = sub_tree.insert(&"home/kitchen/#".parse().unwrap(), "sub_4");
        let sub_5 = sub_tree.insert(&"home/kitchen/+".parse().unwrap(), "sub_5");
        let sub_6 = sub_tree.insert(&"home/kitchen/+".parse().unwrap(), "sub_6");
        let sub_7 = sub_tree.insert(&"#".parse().unwrap(), "sub_7");

        assert!(!sub_tree.is_empty());

        assert!(sub_tree.remove(&"#".parse().unwrap(), sub_1).is_none());

        assert_eq!(
            sub_tree.remove(&"home/kitchen/temperature".parse().unwrap(), sub_1).unwrap(),
            "sub_1"
        );
        assert_eq!(
            sub_tree.remove(&"home/kitchen/temperature".parse().unwrap(), sub_2).unwrap(),
            "sub_2"
        );
        assert_eq!(sub_tree.remove(&"home/kitchen/#".parse().unwrap(), sub_4).unwrap(), "sub_4");
        assert_eq!(sub_tree.remove(&"home/kitchen/+".parse().unwrap(), sub_5).unwrap(), "sub_5");
        assert_eq!(
            sub_tree.remove(&"home/kitchen/humidity".parse().unwrap(), sub_3).unwrap(),
            "sub_3"
        );
        assert_eq!(sub_tree.remove(&"#".parse().unwrap(), sub_7).unwrap(), "sub_7");
        assert_eq!(sub_tree.remove(&"home/kitchen/+".parse().unwrap(), sub_6).unwrap(), "sub_6");

        assert!(sub_tree.is_empty());

        assert!(sub_tree.remove(&"home/kitchen/+".parse().unwrap(), sub_6).is_none());
    }

    #[test]
    fn test_leading_dollar() {
        let mut sub_tree = SubscriptionTree::new();
        sub_tree.insert(&"$SYS/in-flight-message-count".parse().unwrap(), "sub_1");
        sub_tree.insert(&"$SYS/num-connections".parse().unwrap(), "sub_2");
        sub_tree.insert(&"$SYS/#".parse().unwrap(), "sub_3");
        sub_tree.insert(&"$SYS/+/stats".parse().unwrap(), "sub_3_1");
        sub_tree.insert(&"$share/group_a/home/kitchen".parse().unwrap(), "sub_4");
        sub_tree.insert(&"#".parse().unwrap(), "sub_5");
        sub_tree.insert(&"+".parse().unwrap(), "sub_6");
        sub_tree.insert(&"/".parse().unwrap(), "sub_7");
        sub_tree.insert(&"+/monitor/Clients".parse().unwrap(), "sub_8");
        sub_tree.insert(&"$SYS/monitor/+".parse().unwrap(), "sub_9");

        // sub_10 gets everything.
        sub_tree.insert(&"#".parse().unwrap(), "sub_10");
        sub_tree.insert(&"$SYS/#".parse().unwrap(), "sub_10");

        sub_tree.insert(&"$whatever".parse().unwrap(), "sub_11");

        assert!(!sub_tree.is_empty());
        assert_subscribers(&sub_tree, "home", &["sub_5", "sub_6", "sub_10"]);
        assert_subscribers(&sub_tree, "$whatever", &["sub_11"]);
        assert_subscribers(&sub_tree, "$nothing", &[]);
        assert_subscribers(&sub_tree, "$SYS/monitor/Clients", &["sub_3", "sub_9", "sub_10"]);
        assert_subscribers(&sub_tree, "$SYS/num-connections", &["sub_2", "sub_3", "sub_10"]);
        assert_subscribers(&sub_tree, "$SYS/server/stats", &["sub_3", "sub_3_1", "sub_10"]);
    }
}
