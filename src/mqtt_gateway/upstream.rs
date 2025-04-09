use pingora_load_balancing::discovery::Static;
use pingora_load_balancing::selection::Consistent;
use pingora_load_balancing::{Backend, Backends, LoadBalancer};
use std::collections::BTreeSet;
use std::sync::Arc;

pub(crate) async fn create_lb() -> Arc<LoadBalancer<Consistent>> {
    // TODO: implement k8s or configuration discovery.
    let set = BTreeSet::from([Backend::new("127.0.0.1:1883").unwrap()]);
    let discovery = Static::new(set);
    let lb = Arc::new(LoadBalancer::from_backends(Backends::new(discovery)));

    // TODO: schedule service discovery and health check (send ping packet).
    lb.update().await.unwrap();

    lb
}
