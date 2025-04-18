use pingora_load_balancing::discovery::Static;
use pingora_load_balancing::prelude::TcpHealthCheck;
use pingora_load_balancing::selection::Consistent;
use pingora_load_balancing::{Backend, Backends, LoadBalancer};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) fn create_lb() -> Arc<LoadBalancer<Consistent>> {
    // TODO: implement k8s or configuration discovery.
    let backend_set = match std::env::var("BACKEND") {
        Ok(val) => val.split(",").map(|s|Backend::new(s.trim()).unwrap()).collect(),
        Err(_) => BTreeSet::from([Backend::new("127.0.0.1:1883").unwrap()]),
    };
    let mut backends = Backends::new(Static::new(backend_set));
    backends.set_health_check(TcpHealthCheck::new());
    
    let mut lb = LoadBalancer::from_backends(backends);
    lb.update_frequency = Some(Duration::from_secs(60));
    lb.health_check_frequency = Some(Duration::from_secs(60));
    lb.parallel_health_check = true;
    let lb = Arc::new(lb);

    // TODO: schedule service discovery and health check (send ping packet).
    let lb_clone = lb.clone();
    ntex::rt::spawn_fn(|| async move {
        let lb = lb_clone;
        
        const NEVER: Duration = Duration::from_secs(u32::MAX as u64);
        let mut now = Instant::now();
        
        // run update and health check once
        let mut next_update = now;
        let mut next_health_check = now;
        loop {
            // TODO: check if shutdown.

            if next_update <= now {
                // TODO: log err
                let _ = lb.update().await;
                next_update = now + lb.update_frequency.unwrap_or(NEVER);
            }

            if next_health_check <= now {
                lb.backends()
                    .run_health_check(lb.parallel_health_check)
                    .await;
                next_health_check = now + lb.health_check_frequency.unwrap_or(NEVER);
            }

            if lb.update_frequency.is_none() && lb.health_check_frequency.is_none() {
                return;
            }
            let to_wake = std::cmp::min(next_update, next_health_check);
            tokio::time::sleep_until(to_wake.into()).await;
            now = Instant::now();
        }
    });
    
    lb
}
