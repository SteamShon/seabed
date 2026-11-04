use seabed_adapter_objectstore_mem::{
    CountingObjectStore, FaultConfig, FaultInjecting, MemObjectStore,
};
use seabed_conformance::run_object_store_suite;

#[test]
fn mem_store_conforms() {
    run_object_store_suite(MemObjectStore::new);
}

#[test]
fn counting_decorator_conforms() {
    run_object_store_suite(|| CountingObjectStore::new(MemObjectStore::new()));
}

#[test]
fn fault_decorator_with_no_faults_conforms() {
    run_object_store_suite(|| FaultInjecting::new(MemObjectStore::new(), FaultConfig::none()));
}
