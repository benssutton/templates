use crate::{
    config::Settings,
    services::{
        data::{DataService, DataServiceTrait},
        health::{HealthService, HealthServiceTrait},
    },
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub health_service: Arc<dyn HealthServiceTrait>,
    pub data_service: Arc<dyn DataServiceTrait>,
}

impl AppState {
    pub fn new(settings: &Settings) -> Self {
        Self {
            health_service: Arc::new(HealthService::new(settings.clone())),
            data_service: Arc::new(DataService::new(settings.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_wires_services() {
        let state = AppState::new(&Settings::default());
        assert_eq!(state.health_service.get_status(), "running");
    }
}
