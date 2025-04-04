use std::cell::RefCell;
use ntex::util::ByteString;

#[derive(Debug, Default, Clone)]
pub struct SessionState {
    pub client_id: String,
    pub subscriptions: RefCell<Vec<ByteString>>,
}