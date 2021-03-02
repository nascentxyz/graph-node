use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Recursivelly find directories that contains a `subgraph.yaml` file.
fn discover_test_directories(dir: &Path, max_depth: u8) -> io::Result<HashSet<PathBuf>> {
    let mut found_directories: HashSet<PathBuf> = HashSet::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() && max_depth > 0 {
                let new_depth = max_depth - 1;
                found_directories.extend(discover_test_directories(&path, new_depth)?)
            } else if path.file_name() == Some(OsStr::new("subgraph.yaml")) {
                found_directories.insert(dir.into());
                continue;
            }
        }
    }
    Ok(found_directories)
}

#[derive(Debug, Clone, Copy)]
enum TestService {
    Postgres,
    IPFS,
}

struct Docker {
    service: TestService,
}

impl Docker {
    fn start(service: TestService) -> Self {
        println!("Starting service container for: {:?}", service);
        Self { service }
    }
}

impl Drop for Docker {
    fn drop(&mut self) {
        println!("Stopping service container for: {:?}", self.service);
    }
}

#[test]
fn parallel_integration_tests() {
    let current_working_directory =
        std::env::current_dir().expect("failed to identify working directory");
    let integration_tests_root_directory = current_working_directory.join("integration-tests");
    let integration_tests_directories =
        discover_test_directories(&integration_tests_root_directory, 1)
            .expect("failed to discover integration test directories");

    println!(
        "Found {} integration test directories:",
        integration_tests_directories.len()
    );
    for dir in &integration_tests_directories {
        println!(
            "  - {}",
            dir.file_name().map(OsStr::to_string_lossy).unwrap()
        );
    }

    // start docker containers for Postgres and IPFS
    let _postgres = Docker::start(TestService::Postgres);
    let _ipfs = Docker::start(TestService::IPFS);

    // run each test
    for dir in &integration_tests_directories {
        println!(
            "running test for: {}",
            dir.file_name().map(OsStr::to_string_lossy).unwrap()
        );
    }
}
