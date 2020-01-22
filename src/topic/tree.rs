use crate::topic::{
    filter::{TopicFilter, TopicLevel},
    MULTI_LEVEL_WILDCARD_STR, SINGLE_LEVEL_WILDCARD_STR, TOPIC_SEPARATOR,
};
use std::collections::{hash_map::Entry, HashMap};

// TODO(bschwind) - Support shared subscriptions

#[derive(Debug)]
pub struct SubscriptionTree<T> {
    subscribers: Vec<T>,
    single_level_wildcards: Option<Box<SubscriptionTree<T>>>,
    multi_level_wildcards: Vec<T>,
    concrete_topic_levels: HashMap<String, SubscriptionTree<T>>,
}

// TODO(bschwind) - All these topic strings need validation before
//                  operating on them.

impl<T: std::fmt::Debug + PartialEq> SubscriptionTree<T> {
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            single_level_wildcards: None,
            multi_level_wildcards: Vec::new(),
            concrete_topic_levels: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        return self.subscribers.is_empty()
            && self.single_level_wildcards.is_none()
            && self.multi_level_wildcards.is_empty()
            && self.concrete_topic_levels.is_empty();
    }

    pub fn insert(&mut self, topic_filter: TopicFilter, value: T) {
        let mut current_tree = self;
        let mut multi_level = false;

        for level in topic_filter.levels() {
            match level {
                TopicLevel::SingleLevelWildcard => {
                    if current_tree.single_level_wildcards.is_some() {
                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    } else {
                        current_tree.single_level_wildcards =
                            Some(Box::new(SubscriptionTree::new()));
                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    }
                },
                TopicLevel::MultiLevelWildcard => {
                    multi_level = true;
                    break;
                },
                TopicLevel::Concrete(concrete_topic_level) => {
                    if current_tree.concrete_topic_levels.contains_key(concrete_topic_level) {
                        current_tree = current_tree
                            .concrete_topic_levels
                            .get_mut(concrete_topic_level)
                            .unwrap();
                    } else {
                        current_tree
                            .concrete_topic_levels
                            .insert(concrete_topic_level.to_string(), SubscriptionTree::new());

                        // TODO - Do this without another hash lookup
                        current_tree = current_tree
                            .concrete_topic_levels
                            .get_mut(concrete_topic_level)
                            .unwrap();
                    }
                },
            }
        }

        if multi_level {
            current_tree.multi_level_wildcards.push(value);
        } else {
            current_tree.subscribers.push(value);
        }
    }

    pub fn remove(&mut self, topic_filter: String, value: T) -> Option<T> {
        let mut current_tree = self;
        let mut stack: Vec<(*mut SubscriptionTree<T>, usize)> = vec![];

        let levels: Vec<&str> = topic_filter.split(TOPIC_SEPARATOR).collect();
        let mut level_index = 0;

        for level in &levels {
            match *level {
                SINGLE_LEVEL_WILDCARD_STR => {
                    if current_tree.single_level_wildcards.is_some() {
                        stack.push((&mut *current_tree, level_index));
                        level_index += 1;

                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    } else {
                        return None;
                    }
                },
                MULTI_LEVEL_WILDCARD_STR => {
                    break;
                },
                concrete_topic_level => {
                    if current_tree.concrete_topic_levels.contains_key(concrete_topic_level) {
                        stack.push((&mut *current_tree, level_index));
                        level_index += 1;

                        current_tree = current_tree
                            .concrete_topic_levels
                            .get_mut(concrete_topic_level)
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

            if *level == MULTI_LEVEL_WILDCARD_STR {
                if let Some(pos) =
                    current_tree.multi_level_wildcards.iter().position(|x| *x == value)
                {
                    Some(current_tree.multi_level_wildcards.remove(pos))
                } else {
                    None
                }
            } else {
                if let Some(pos) = current_tree.subscribers.iter().position(|x| *x == value) {
                    Some(current_tree.subscribers.remove(pos))
                } else {
                    None
                }
            }
        };

        // Go up the stack, cleaning up empty nodes
        while let Some((stack_val, level_index)) = stack.pop() {
            let mut tree = unsafe { &mut *stack_val };

            let level = levels[level_index];

            match level {
                SINGLE_LEVEL_WILDCARD_STR => {
                    if tree.single_level_wildcards.as_ref().map(|t| t.is_empty()).unwrap_or(false) {
                        tree.single_level_wildcards = None;
                    }
                },
                MULTI_LEVEL_WILDCARD_STR => {
                    // TODO - Ignore this case?
                },
                concrete_topic_level => {
                    if let Entry::Occupied(o) =
                        tree.concrete_topic_levels.entry(concrete_topic_level.to_string())
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

    pub fn matching_subscribers<'a, F: FnMut(&T)>(&'a self, topic_name: &str, mut sub_fn: F) {
        let mut tree_stack = vec![];
        let levels: Vec<&str> = topic_name.split(TOPIC_SEPARATOR).collect();

        tree_stack.push((self, 0));

        while !tree_stack.is_empty() {
            let (current_tree, current_level) = tree_stack.pop().unwrap();
            let level = levels[current_level];

            for subscriber in &current_tree.multi_level_wildcards {
                sub_fn(subscriber);
            }

            if let Some(sub_tree) = &current_tree.single_level_wildcards {
                if current_level + 1 < levels.len() {
                    tree_stack.push((sub_tree, current_level + 1));
                } else {
                    for subscriber in &sub_tree.subscribers {
                        sub_fn(subscriber);
                    }
                }
            }

            if current_tree.concrete_topic_levels.contains_key(level) {
                let sub_tree = current_tree.concrete_topic_levels.get(level).unwrap();

                if current_level + 1 < levels.len() {
                    let sub_tree = current_tree.concrete_topic_levels.get(level).unwrap();
                    tree_stack.push((sub_tree, current_level + 1));
                } else {
                    for subscriber in &sub_tree.subscribers {
                        sub_fn(subscriber);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::topic::SubscriptionTree;

    #[test]
    fn test_insert() {
        let mut sub_tree = SubscriptionTree::new();
        sub_tree.insert("home/kitchen/temperature".parse().unwrap(), 1);
        sub_tree.insert("home/kitchen/humidity".parse().unwrap(), 2);
        sub_tree.insert("home/kitchen".parse().unwrap(), 3);
        sub_tree.insert("home/+/humidity".parse().unwrap(), 4);
        sub_tree.insert("home/+".parse().unwrap(), 5);
        sub_tree.insert("home/#".parse().unwrap(), 6);
        sub_tree.insert("home/+/temperature".parse().unwrap(), 7);
        sub_tree.insert("office/stairwell/temperature".parse().unwrap(), 8);
        sub_tree.insert("office/+/+".parse().unwrap(), 9);
        sub_tree.insert("office/+/+/some_desk/+/fan_speed/+/temperature".parse().unwrap(), 10);
        sub_tree.insert("office/+/+/some_desk/+/#".parse().unwrap(), 11);
        sub_tree.insert("sport/tennis/+".parse().unwrap(), 21);
        sub_tree.insert("#".parse().unwrap(), 12);

        println!("{:#?}", sub_tree);

        sub_tree.matching_subscribers("home/kitchen", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers("home/kitchen/humidity", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers("office/stairwell/temperature", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers(
            "office/tokyo/shibuya/some_desk/cpu_1/fan_speed/blade_4/temperature",
            |s| {
                println!("{}", s);
            },
        );

        println!();

        sub_tree.matching_subscribers("home", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers("sport/tennis/player1", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers("sport/tennis/player2", |s| {
            println!("{}", s);
        });

        println!();

        sub_tree.matching_subscribers("sport/tennis/player1/ranking", |s| {
            println!("{}", s);
        });
    }

    #[test]
    fn test_remove() {
        let mut sub_tree = SubscriptionTree::new();
        sub_tree.insert("home/kitchen/temperature".parse().unwrap(), 1);
        sub_tree.insert("home/kitchen/temperature".parse().unwrap(), 2);
        sub_tree.insert("home/kitchen/humidity".parse().unwrap(), 1);
        sub_tree.insert("home/kitchen/#".parse().unwrap(), 1);
        sub_tree.insert("home/kitchen/+".parse().unwrap(), 3);
        sub_tree.insert("home/kitchen/+".parse().unwrap(), 2);
        sub_tree.insert("#".parse().unwrap(), 6);

        println!("{:#?}", sub_tree);

        sub_tree.remove("home/kitchen/temperature".to_string(), 1);
        println!("{:#?}", sub_tree);

        sub_tree.remove("home/kitchen/temperature".to_string(), 2);
        println!("{:#?}", sub_tree);

        sub_tree.remove("home/kitchen/#".to_string(), 1);
        println!("{:#?}", sub_tree);

        sub_tree.remove("home/kitchen/+".to_string(), 3);
        println!("{:#?}", sub_tree);
    }
}
