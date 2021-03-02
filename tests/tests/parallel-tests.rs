use bollard::{container, Docker};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tokio::time::{sleep, Duration};

const POSTGRES_IMAGE: &'static str = "postgres";
const IPFS_IMAGE: &'static str = "ipfs/go-ipfs:v0.4.23";

type DockerError = bollard::errors::Error;

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
enum TestContainerService {
    Postgres,
    Ipfs,
}

fn build_postgres_container() -> container::Config<&'static str> {
    container::Config {
        image: Some(POSTGRES_IMAGE),
        env: Some(vec!["POSTGRES_PASSWORD=password", "POSTGRES_USER=postgres"]),
        ..Default::default()
    }
}

fn build_ipfs_container_config() -> container::Config<&'static str> {
    container::Config {
        image: Some(IPFS_IMAGE),
        ..Default::default()
    }
}

impl TestContainerService {
    fn config(&self) -> container::Config<&'static str> {
        match &self {
            TestContainerService::Postgres => build_postgres_container(),
            TestContainerService::Ipfs => build_ipfs_container_config(),
        }
    }

    fn options(&self) -> container::CreateContainerOptions<&'static str> {
        container::CreateContainerOptions { name: self.name() }
    }

    fn name(&self) -> &'static str {
        match &self {
            TestContainerService::Postgres => "graph_node_integration_test_postgres",
            TestContainerService::Ipfs => "graph_node_integration_test_ipfs",
        }
    }
}

struct DockerTestClient {
    service: TestContainerService,
    client: Docker,
}

impl DockerTestClient {
    async fn start(service: TestContainerService) -> Result<Self, DockerError> {
        println!("Creating service container for: {}", service.name());
        let client =
            Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

        // try to remove the container if it already exists
        client.remove_container(service.name(), None).await?;

        // create docker container
        client
            .create_container(Some(service.options()), service.config())
            .await?;

        // start docker container
        println!("Starting service container for: {}", service.name());
        client
            .start_container::<&'static str>(service.name(), None)
            .await?;

        Ok(Self { service, client })
    }

    async fn stop(&self) -> Result<(), DockerError> {
        println!("Stopping service container for: {}", self.service.name());
        self.client
            .kill_container::<&'static str>(self.service.name(), None)
            .await
    }
}

#[tokio::test]
async fn parallel_integration_tests() {
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

    // start docker containers for Postgres and Ipfs
    let postgres = DockerTestClient::start(TestContainerService::Postgres)
        .await
        .expect("failed to start container service for Postgres.");
    let ipfs = DockerTestClient::start(TestContainerService::Ipfs)
        .await
        .expect("failed to start container service for IPFS.");

    // run each test
    for dir in &integration_tests_directories {
        println!(
            "running test for: {}",
            dir.file_name().map(OsStr::to_string_lossy).unwrap()
        );
        sleep(Duration::from_millis(100)).await; // TODO: run actual tests here
    }

    // Stop containers.
    postgres
        .stop()
        .await
        .expect("failed to stop container service for Postgres");
    ipfs.stop()
        .await
        .expect("failed to stop container service for IPFS");
}
