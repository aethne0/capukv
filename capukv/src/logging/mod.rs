use std::sync::atomic::AtomicU64;

use tracing::{Event, Subscriber};
use tracing_subscriber::{Layer, layer::Context};

pub struct TroopTraceSubscriber {
    uri: String,
    id: AtomicU64,
    uuid: uuid::Uuid,
}

impl TroopTraceSubscriber {
    #[must_use]
    pub fn new(id: uuid::Uuid, uri: String) -> Self {
        Self { uri, id: AtomicU64::new(1), uuid: id }
    }
}

fn int_from_level(level: &tracing::Level) -> u64 {
    match *level {
        tracing::Level::TRACE => 0,
        tracing::Level::DEBUG => 1,
        tracing::Level::INFO => 2,
        tracing::Level::WARN => 3,
        tracing::Level::ERROR => 4,
    }
}

impl<S: Subscriber> Layer<S> for TroopTraceSubscriber {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // very simple static json structure, we can avoid a dependency
        let level_field = format!("\"level\": {}", int_from_level(event.metadata().level()));
        let log_id_field = format!("\"log_id\": {}", self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

        let msg_field = format!("\"msg\": \"{}\"", message_from_event(event));
        let node_id_field = format!("\"node_uuid\": \"{}\"", self.uuid.as_hyphenated());

        let msg = format!("{{{}}}", vec![level_field, log_id_field, msg_field, node_id_field].join(","));

        let client = reqwest::Client::new();
        let uri = self.uri.clone();

        tokio::spawn(async move {
            if let Err(e) = client.post(uri).body(msg).send().await {
                eprintln!("{}", e.to_string());
            }
        });
    }
}

use tracing::field::{Field, Visit};

struct MsgVisitor {
    pub message: Option<String>,
}

impl Visit for MsgVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }
}

fn message_from_event(event: &Event<'_>) -> String {
    let mut visitor = MsgVisitor { message: None };
    event.record(&mut visitor);
    visitor.message.unwrap()
}
