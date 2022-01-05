use mqtt_v5::topic::{Topic, TopicFilter, TopicLevel};
use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug)]
pub struct RetainedMessageTreeNode<T> {
    retained_data: Option<T>,
    concrete_topic_levels: HashMap<String, RetainedMessageTreeNode<T>>,
}

#[derive(Debug)]
pub struct RetainedMessageTree<T> {
    root: RetainedMessageTreeNode<T>,
}

impl<T: std::fmt::Debug> RetainedMessageTree<T> {
    pub fn new() -> Self {
        Self { root: RetainedMessageTreeNode::new() }
    }

    pub fn insert(&mut self, topic: &Topic, retained_data: T) {
        self.root.insert(topic, retained_data);
    }

    /// Get the retained messages which match a given topic filter.
    pub fn retained_messages(&self, topic_filter: &TopicFilter) -> impl Iterator<Item = &T> {
        self.root.retained_messages(topic_filter)
    }

    pub fn remove(&mut self, topic: &Topic) -> Option<T> {
        self.root.remove(topic)
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.root.is_empty()
    }
}

impl<T: std::fmt::Debug> RetainedMessageTreeNode<T> {
    fn new() -> Self {
        Self { retained_data: None, concrete_topic_levels: HashMap::new() }
    }

    fn is_empty(&self) -> bool {
        self.retained_data.is_none() && self.concrete_topic_levels.is_empty()
    }

    fn insert(&mut self, topic: &Topic, retained_data: T) {
        let mut current_tree = self;

        for level in topic.levels() {
            match level {
                TopicLevel::SingleLevelWildcard | TopicLevel::MultiLevelWildcard => {
                    unreachable!("Publish topics only contain concrete levels");
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if !current_tree.concrete_topic_levels.contains_key(concrete_topic_level) {
                        current_tree.concrete_topic_levels.insert(
                            concrete_topic_level.to_string(),
                            RetainedMessageTreeNode::new(),
                        );
                    }

                    // TODO - Do this without another hash lookup
                    current_tree =
                        current_tree.concrete_topic_levels.get_mut(concrete_topic_level).unwrap();
                },
            }
        }

        current_tree.retained_data = Some(retained_data);
    }

    fn remove(&mut self, topic: &Topic) -> Option<T> {
        let mut current_tree = self;
        let mut stack: Vec<(*mut RetainedMessageTreeNode<T>, usize)> = vec![];

        let levels: Vec<TopicLevel> = topic.levels().collect();
        let mut level_index = 0;

        for level in &levels {
            match level {
                TopicLevel::SingleLevelWildcard | TopicLevel::MultiLevelWildcard => {
                    unreachable!("Publish topics only contain concrete levels");
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

        let return_val = current_tree.retained_data.take();

        // Go up the stack, cleaning up empty nodes
        while let Some((stack_val, level_index)) = stack.pop() {
            let tree = unsafe { &mut *stack_val };

            let level = &levels[level_index];

            match level {
                TopicLevel::SingleLevelWildcard | TopicLevel::MultiLevelWildcard => {
                    unreachable!("Publish topics only contain concrete levels");
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

        return_val
    }

    pub fn retained_messages(&self, topic_filter: &TopicFilter) -> impl Iterator<Item = &T> {
        let mut retained_messages = Vec::new();
        let mut tree_stack = vec![(self, 0)];
        let mut multi_level = false;
        let levels: Vec<TopicLevel> = topic_filter.levels().collect();

        while !tree_stack.is_empty() {
            let (current_tree, current_level) = tree_stack.pop().unwrap();

            if multi_level {
                // Add all the retained messages and keep going.
                for sub_tree in current_tree.concrete_topic_levels.values() {
                    if let Some(retained_data) = sub_tree.retained_data.as_ref() {
                        retained_messages.push(retained_data);
                    }

                    tree_stack.push((sub_tree, current_level + 1));
                }

                continue;
            }

            let level = &levels[current_level];

            match level {
                TopicLevel::SingleLevelWildcard => {
                    for sub_tree in current_tree.concrete_topic_levels.values() {
                        if current_level + 1 < levels.len() {
                            tree_stack.push((sub_tree, current_level + 1));
                        } else {
                            if let Some(retained_data) = sub_tree.retained_data.as_ref() {
                                retained_messages.push(retained_data);
                            }
                        }
                    }
                },
                TopicLevel::MultiLevelWildcard => {
                    multi_level = true;

                    for sub_tree in current_tree.concrete_topic_levels.values() {
                        if let Some(retained_data) = sub_tree.retained_data.as_ref() {
                            retained_messages.push(retained_data);
                        }

                        tree_stack.push((sub_tree, current_level + 1));
                    }
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if current_tree.concrete_topic_levels.contains_key(*concrete_topic_level) {
                        let sub_tree =
                            current_tree.concrete_topic_levels.get(*concrete_topic_level).unwrap();

                        if current_level + 1 < levels.len() {
                            let sub_tree = current_tree
                                .concrete_topic_levels
                                .get(*concrete_topic_level)
                                .unwrap();
                            tree_stack.push((sub_tree, current_level + 1));
                        } else {
                            if let Some(retained_data) = sub_tree.retained_data.as_ref() {
                                retained_messages.push(retained_data);
                            }
                        }
                    }
                },
            }
        }

        retained_messages.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::retained::RetainedMessageTree;

    #[test]
    fn test_insert() {
        let mut sub_tree = RetainedMessageTree::new();
        sub_tree.insert(&"home/kitchen/temperature".parse().unwrap(), 1);
        sub_tree.insert(&"home/bedroom/temperature".parse().unwrap(), 2);
        sub_tree.insert(&"home/kitchen".parse().unwrap(), 7);

        sub_tree.insert(&"office/cafe".parse().unwrap(), 12);
        sub_tree.insert(&"office/cafe/temperature".parse().unwrap(), 27);

        for msg in sub_tree.retained_messages(&"+/+/temperature".parse().unwrap()) {
            dbg!(msg);
        }

        assert_eq!(sub_tree.remove(&"home/kitchen/temperature".parse().unwrap()), Some(1));
        assert_eq!(sub_tree.remove(&"home/kitchen".parse().unwrap()), Some(7));
        assert_eq!(sub_tree.remove(&"home/kitchen".parse().unwrap()), None);
        dbg!(sub_tree);
    }
}
