use crate::topic::{
    MAX_TOPIC_LEN_BYTES, MULTI_LEVEL_WILDCARD, SHARED_SUBSCRIPTION_PREFIX, SINGLE_LEVEL_WILDCARD,
    TOPIC_SEPARATOR,
};
use std::str::FromStr;

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

#[cfg(test)]
mod tests {
    use crate::topic::{TopicFilter, TopicParseError, MAX_TOPIC_LEN_BYTES};

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
}
