mod gateway;
mod gateway_class;
mod http_route;

pub use gateway::controller as gateway_controller;
pub use gateway_class::controller as gateway_class_controller;
pub use http_route::controller as http_route_controller;

pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
