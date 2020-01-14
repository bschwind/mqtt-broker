use std::collections::HashMap;

#[derive(Debug)]
pub struct SubscriptionTrie<T> {
    subscribers: Vec<T>,
    single_level_wildcards: Option<Box<SubscriptionTrie<T>>>,
    multi_level_wildcards: Vec<T>,
    concrete_topic_levels: HashMap<String, SubscriptionTrie<T>>,
}

// TODO(bschwind) - All these topic strings need validation before
//                  operating on them.

impl<T> SubscriptionTrie<T> {
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            single_level_wildcards: None,
            multi_level_wildcards: Vec::new(),
            concrete_topic_levels: HashMap::new(),
        }
    }

    pub fn insert(&mut self, topic_filter: String, value: T) {
        let mut current_trie = self;
        let mut multi_level = false;

        for level in topic_filter.split("/") {
            match level {
                "+" => {
                    if current_trie.single_level_wildcards.is_some() {
                        current_trie = current_trie.single_level_wildcards.as_mut().unwrap();
                    } else {
                        current_trie.single_level_wildcards =
                            Some(Box::new(SubscriptionTrie::new()));
                        current_trie = current_trie.single_level_wildcards.as_mut().unwrap();
                    }
                },
                "#" => {
                    multi_level = true;
                    break;
                },
                concrete_topic_level => {
                    if current_trie.concrete_topic_levels.contains_key(concrete_topic_level) {
                        current_trie = current_trie
                            .concrete_topic_levels
                            .get_mut(concrete_topic_level)
                            .unwrap();
                    } else {
                        current_trie
                            .concrete_topic_levels
                            .insert(concrete_topic_level.to_string(), SubscriptionTrie::new());

                        // TODO - Do this without another hash lookup
                        current_trie = current_trie
                            .concrete_topic_levels
                            .get_mut(concrete_topic_level)
                            .unwrap();
                    }
                },
            }
        }

        if multi_level {
            current_trie.multi_level_wildcards.push(value);
        } else {
            current_trie.subscribers.push(value);
        }
    }

    pub fn matching_subscribers<'a, F: FnMut(&T)>(&'a self, topic_name: String, mut sub_fn: F) {
        let mut trie_stack = vec![];
        let levels: Vec<&str> = topic_name.split("/").collect();

        trie_stack.push((self, 0));

        while !trie_stack.is_empty() {
            let (current_trie, current_level) = trie_stack.pop().unwrap();
            let level = levels[current_level];

            for subscriber in &current_trie.multi_level_wildcards {
                sub_fn(subscriber);
            }

            if let Some(sub_trie) = &current_trie.single_level_wildcards {
                if current_level + 1 < levels.len() {
                    trie_stack.push((sub_trie, current_level + 1));
                } else {
                    for subscriber in &sub_trie.subscribers {
                        sub_fn(subscriber);
                    }
                }
            }

            if current_trie.concrete_topic_levels.contains_key(level) {
                let sub_trie = current_trie.concrete_topic_levels.get(level).unwrap();

                if current_level + 1 < levels.len() {
                    let sub_trie = current_trie.concrete_topic_levels.get(level).unwrap();
                    trie_stack.push((sub_trie, current_level + 1));
                } else {
                    for subscriber in &sub_trie.subscribers {
                        sub_fn(subscriber);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::topic::SubscriptionTrie;

    #[test]
    fn test_insert() {
        let mut sub_trie = SubscriptionTrie::new();
        sub_trie.insert("home/kitchen/temperature".to_string(), 1);
        sub_trie.insert("home/kitchen/humidity".to_string(), 2);
        sub_trie.insert("home/kitchen".to_string(), 3);
        sub_trie.insert("home/+/humidity".to_string(), 4);
        sub_trie.insert("home/+".to_string(), 5);
        sub_trie.insert("home/#".to_string(), 6);
        sub_trie.insert("home/+/temperature".to_string(), 7);
        sub_trie.insert("office/stairwell/temperature".to_string(), 8);
        sub_trie.insert("office/+/+".to_string(), 9);
        sub_trie.insert("office/+/+/some_desk/+/fan_speed/+/temperature".to_string(), 10);
        sub_trie.insert("office/+/+/some_desk/+/#".to_string(), 11);
        sub_trie.insert("#".to_string(), 12);

        println!("{:#?}", sub_trie);

        sub_trie.matching_subscribers("home/kitchen".to_string(), |s| {
            println!("{}", s);
        });

        println!();

        sub_trie.matching_subscribers("home/kitchen/humidity".to_string(), |s| {
            println!("{}", s);
        });

        println!();

        sub_trie.matching_subscribers("office/stairwell/temperature".to_string(), |s| {
            println!("{}", s);
        });

        println!();

        sub_trie.matching_subscribers(
            "office/tokyo/shibuya/some_desk/cpu_1/fan_speed/blade_4/temperature".to_string(),
            |s| {
                println!("{}", s);
            },
        );

        println!();

        sub_trie.matching_subscribers("home".to_string(), |s| {
            println!("{}", s);
        });
    }
}
