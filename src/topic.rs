use std::{
    collections::{hash_map::Entry, HashMap},
    str::FromStr,
};

const TOPIC_SEPARATOR: char = '/';

const MULTI_LEVEL_WILDCARD: char = '#';
const MULTI_LEVEL_WILDCARD_STR: &'static str = "#";

const SINGLE_LEVEL_WILDCARD: char = '+';
const SINGLE_LEVEL_WILDCARD_STR: &'static str = "+";

const SHARED_SUBSCRIPTION_PREFIX: &'static str = "$share/";

pub const MAX_TOPIC_LEN_BYTES: usize = 65_535;

// TODO(bschwind) - Support shared subscriptions

/// A filter for subscribers to indicate which topics they want
/// to receive messages from. Can contain wildcards.
#[derive(Debug, PartialEq)]
pub enum TopicFilter {
    Concrete { filter: String, level_count: u32 },
    Wildcard { filter: String, level_count: u32 },
    SharedConcrete { group_name: String, filter: String, level_count: u32 },
    SharedWildcard { group_name: String, filter: String, level_count: u32 },
}

/// A topic name publishers use when sending MQTT messages.
/// Cannot contain wildcards.
#[derive(Debug)]
pub struct Topic {
    topic_name: String,
    level_count: u32,
}

#[derive(Debug, PartialEq)]
pub enum TopicParseError {
    EmptyTopic,
    TopicTooLong,
    MultilevelWildcardNotAtEnd,
    InvalidWildcardLevel,
    InvalidSharedGroupName,
    EmptySharedGroupName,
}

/// If Ok, returns (level_count, contains_wildcards).
fn process_filter(filter: &str) -> Result<(u32, bool), TopicParseError> {
    let mut level_count = 0;
    let mut contains_wildcards = false;
    for level in filter.split(TOPIC_SEPARATOR) {
        let level_contains_wildcard =
            level.contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD);
        if level_contains_wildcard {
            // Any wildcards on a particular level must be specified on their own
            if level.len() > 1 {
                return Err(TopicParseError::InvalidWildcardLevel);
            }

            contains_wildcards = true;
        }

        level_count += 1;
    }

    Ok((level_count, contains_wildcards))
}

impl FromStr for TopicFilter {
    type Err = TopicParseError;

    fn from_str(filter: &str) -> Result<Self, Self::Err> {
        // Filters and topics cannot be empty
        if filter.is_empty() {
            return Err(TopicParseError::EmptyTopic);
        }

        // Filters cannot exceed the byte length in the MQTT spec
        if filter.len() > MAX_TOPIC_LEN_BYTES {
            return Err(TopicParseError::TopicTooLong);
        }

        // Multi-level wildcards can only be at the end of the topic
        if let Some(pos) = filter.rfind(MULTI_LEVEL_WILDCARD) {
            if pos != filter.len() - 1 {
                return Err(TopicParseError::MultilevelWildcardNotAtEnd);
            }
        }

        let mut shared_group = None;

        if filter.starts_with(SHARED_SUBSCRIPTION_PREFIX) {
            let filter_rest = &filter[SHARED_SUBSCRIPTION_PREFIX.len()..];

            if filter_rest.is_empty() {
                return Err(TopicParseError::EmptySharedGroupName);
            }

            if let Some(slash_pos) = filter_rest.find(TOPIC_SEPARATOR) {
                let shared_name = &filter_rest[0..slash_pos];

                // slash_pos+1 is safe here, we've already validated the string
                // has a nonzero length.
                let shared_filter = &filter_rest[(slash_pos + 1)..];

                if shared_name.is_empty() {
                    return Err(TopicParseError::EmptySharedGroupName);
                }

                if shared_name
                    .contains(|x: char| x == SINGLE_LEVEL_WILDCARD || x == MULTI_LEVEL_WILDCARD)
                {
                    return Err(TopicParseError::InvalidSharedGroupName);
                }

                if shared_filter.is_empty() {
                    return Err(TopicParseError::EmptyTopic);
                }

                shared_group = Some((shared_name, shared_filter))
            } else {
                return Err(TopicParseError::EmptyTopic);
            }
        }

        let topic_filter = if let Some((group_name, shared_filter)) = shared_group {
            let (level_count, contains_wildcards) = process_filter(shared_filter)?;

            if contains_wildcards {
                TopicFilter::SharedWildcard {
                    group_name: group_name.to_string(),
                    filter: shared_filter.to_string(),
                    level_count,
                }
            } else {
                TopicFilter::SharedConcrete {
                    group_name: group_name.to_string(),
                    filter: shared_filter.to_string(),
                    level_count,
                }
            }
        } else {
            let (level_count, contains_wildcards) = process_filter(filter)?;

            if contains_wildcards {
                TopicFilter::Wildcard { filter: filter.to_string(), level_count }
            } else {
                TopicFilter::Concrete { filter: filter.to_string(), level_count }
            }
        };

        Ok(topic_filter)
    }
}

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

    pub fn insert(&mut self, topic_filter: String, value: T) {
        let mut current_tree = self;
        let mut multi_level = false;

        for level in topic_filter.split(TOPIC_SEPARATOR) {
            match level {
                SINGLE_LEVEL_WILDCARD_STR => {
                    if current_tree.single_level_wildcards.is_some() {
                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    } else {
                        current_tree.single_level_wildcards =
                            Some(Box::new(SubscriptionTree::new()));
                        current_tree = current_tree.single_level_wildcards.as_mut().unwrap();
                    }
                },
                MULTI_LEVEL_WILDCARD_STR => {
                    multi_level = true;
                    break;
                },
                concrete_topic_level => {
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
    use crate::topic::{SubscriptionTree, TopicFilter, TopicParseError, MAX_TOPIC_LEN_BYTES};

    #[test]
    fn test_topic_filter_parse_empty_topic() {
        assert_eq!("".parse::<TopicFilter>().unwrap_err(), TopicParseError::EmptyTopic);
    }

    #[test]
    fn test_topic_filter_parse_length() {
        let just_right_topic = "a".repeat(MAX_TOPIC_LEN_BYTES);
        assert!(just_right_topic.parse::<TopicFilter>().is_ok());

        let too_long_topic = "a".repeat(MAX_TOPIC_LEN_BYTES + 1);
        assert_eq!(
            too_long_topic.parse::<TopicFilter>().unwrap_err(),
            TopicParseError::TopicTooLong
        );
    }

    #[test]
    fn test_topic_filter_parse_concrete() {
        assert_eq!(
            "/".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete { filter: "/".to_string(), level_count: 2 }
        );

        assert_eq!(
            "a".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete { filter: "a".to_string(), level_count: 1 }
        );

        // $SYS topics can be subscribed to, but can't be published
        assert_eq!(
            "home/kitchen".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete { filter: "home/kitchen".to_string(), level_count: 2 }
        );

        assert_eq!(
            "home/kitchen/temperature".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete {
                filter: "home/kitchen/temperature".to_string(),
                level_count: 3,
            }
        );

        assert_eq!(
            "home/kitchen/temperature/celsius".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete {
                filter: "home/kitchen/temperature/celsius".to_string(),
                level_count: 4,
            }
        );
    }

    #[test]
    fn test_topic_filter_parse_single_level_wildcard() {
        assert_eq!(
            "+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "+".to_string(), level_count: 1 }
        );

        assert_eq!(
            "+/".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "+/".to_string(), level_count: 2 }
        );

        assert_eq!(
            "sport/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "sport/+".to_string(), level_count: 2 }
        );

        assert_eq!(
            "/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "/+".to_string(), level_count: 2 }
        );
    }

    #[test]
    fn test_topic_filter_parse_multi_level_wildcard() {
        assert_eq!(
            "#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "#".to_string(), level_count: 1 }
        );

        assert_eq!(
            "#/".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::MultilevelWildcardNotAtEnd
        );

        assert_eq!(
            "/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "/#".to_string(), level_count: 2 }
        );

        assert_eq!(
            "sport/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "sport/#".to_string(), level_count: 2 }
        );

        assert_eq!(
            "home/kitchen/temperature/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard {
                filter: "home/kitchen/temperature/#".to_string(),
                level_count: 4,
            }
        );
    }

    #[test]
    fn test_topic_filter_parse_shared_subscription_concrete() {
        assert_eq!(
            "$share/group_a/home".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedConcrete {
                group_name: "group_a".to_string(),
                filter: "home".to_string(),
                level_count: 1,
            }
        );

        assert_eq!(
            "$share/group_a/home/kitchen/temperature".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedConcrete {
                group_name: "group_a".to_string(),
                filter: "home/kitchen/temperature".to_string(),
                level_count: 3,
            }
        );

        assert_eq!(
            "$share/group_a//".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedConcrete {
                group_name: "group_a".to_string(),
                filter: "/".to_string(),
                level_count: 2,
            }
        );
    }

    #[test]
    fn test_topic_filter_parse_shared_subscription_wildcard() {
        assert_eq!(
            "$share/group_b/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard {
                group_name: "group_b".to_string(),
                filter: "#".to_string(),
                level_count: 1,
            }
        );

        assert_eq!(
            "$share/group_b/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard {
                group_name: "group_b".to_string(),
                filter: "+".to_string(),
                level_count: 1,
            }
        );

        assert_eq!(
            "$share/group_b/+/temperature".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard {
                group_name: "group_b".to_string(),
                filter: "+/temperature".to_string(),
                level_count: 2,
            }
        );

        assert_eq!(
            "$share/group_c/+/temperature/+/meta".parse::<TopicFilter>().unwrap(),
            TopicFilter::SharedWildcard {
                group_name: "group_c".to_string(),
                filter: "+/temperature/+/meta".to_string(),
                level_count: 4,
            }
        );
    }

    #[test]
    fn test_topic_filter_parse_invalid_shared_subscription() {
        assert_eq!(
            "$share/".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );
        assert_eq!("$share/a".parse::<TopicFilter>().unwrap_err(), TopicParseError::EmptyTopic);
        assert_eq!("$share/a/".parse::<TopicFilter>().unwrap_err(), TopicParseError::EmptyTopic);
        assert_eq!(
            "$share//".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );
        assert_eq!(
            "$share///".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::EmptySharedGroupName
        );

        assert_eq!(
            "$share/invalid_group#/#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidSharedGroupName
        );
    }

    #[test]
    fn test_topic_filter_parse_sys_prefix() {
        assert_eq!(
            "$SYS/stats".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete { filter: "$SYS/stats".to_string(), level_count: 2 }
        );

        assert_eq!(
            "/$SYS/stats".parse::<TopicFilter>().unwrap(),
            TopicFilter::Concrete { filter: "/$SYS/stats".to_string(), level_count: 3 }
        );

        assert_eq!(
            "$SYS/+".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "$SYS/+".to_string(), level_count: 2 }
        );

        assert_eq!(
            "$SYS/#".parse::<TopicFilter>().unwrap(),
            TopicFilter::Wildcard { filter: "$SYS/#".to_string(), level_count: 2 }
        );
    }

    #[test]
    fn test_topic_filter_parse_invalid_filters() {
        assert_eq!(
            "sport/#/stats".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::MultilevelWildcardNotAtEnd
        );
        assert_eq!(
            "sport/#/stats#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport#/stats#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport/tennis#".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
        assert_eq!(
            "sport/++".parse::<TopicFilter>().unwrap_err(),
            TopicParseError::InvalidWildcardLevel
        );
    }

    #[test]
    fn test_insert() {
        let mut sub_tree = SubscriptionTree::new();
        sub_tree.insert("home/kitchen/temperature".to_string(), 1);
        sub_tree.insert("home/kitchen/humidity".to_string(), 2);
        sub_tree.insert("home/kitchen".to_string(), 3);
        sub_tree.insert("home/+/humidity".to_string(), 4);
        sub_tree.insert("home/+".to_string(), 5);
        sub_tree.insert("home/#".to_string(), 6);
        sub_tree.insert("home/+/temperature".to_string(), 7);
        sub_tree.insert("office/stairwell/temperature".to_string(), 8);
        sub_tree.insert("office/+/+".to_string(), 9);
        sub_tree.insert("office/+/+/some_desk/+/fan_speed/+/temperature".to_string(), 10);
        sub_tree.insert("office/+/+/some_desk/+/#".to_string(), 11);
        sub_tree.insert("sport/tennis/+".to_string(), 21);
        sub_tree.insert("#".to_string(), 12);

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
        sub_tree.insert("home/kitchen/temperature".to_string(), 1);
        sub_tree.insert("home/kitchen/temperature".to_string(), 2);
        sub_tree.insert("home/kitchen/humidity".to_string(), 1);
        sub_tree.insert("home/kitchen/#".to_string(), 1);
        sub_tree.insert("home/kitchen/+".to_string(), 3);
        sub_tree.insert("home/kitchen/+".to_string(), 2);
        sub_tree.insert("#".to_string(), 6);

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
