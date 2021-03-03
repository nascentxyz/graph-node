use bollard::models::HostConfig;
use bollard::{container, Docker};
use futures::{stream::futures_unordered::FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::process::Command;

/// A counter for uniquely naming Ganache containers
static GANACHE_CONTAINER_COUNT: AtomicUsize = AtomicUsize::new(0);

const POSTGRES_IMAGE: &'static str = "postgres";
const IPFS_IMAGE: &'static str = "ipfs/go-ipfs:v0.4.23";
const GANACHE_IMAGE: &'static str = "trufflesuite/ganache-cli";

type DockerError = bollard::errors::Error;

/// Strip parent directories from filenames
fn basename(path: &Path) -> std::borrow::Cow<'_, str> {
    path.file_name().map(OsStr::to_string_lossy).unwrap()
}

/// Fetches a unique number for naming Ganache containers
fn get_unique_counter() -> u32 {
    let old_ganache_count = GANACHE_CONTAINER_COUNT.fetch_add(1, Ordering::SeqCst);
    (old_ganache_count + 1) as u32
}

/// Recursivelly find directories that contains a `subgraph.yaml` file.
fn discover_test_directories(dir: &Path, max_depth: u8) -> io::Result<HashSet<PathBuf>> {
    let mut found_directories: HashSet<PathBuf> = HashSet::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() && max_depth > 0 {
                let new_depth = max_depth - 1;
                found_directories.extend(discover_test_directories(&path, new_depth)?)
            } else if basename(&path) == "subgraph.yaml" {
                found_directories.insert(dir.into());
                continue;
            }
        }
    }
    Ok(found_directories)
}

#[derive(Debug)]
enum TestContainerService {
    Postgres,
    Ipfs,
    Ganache(u32),
}

fn build_postgres_container() -> container::Config<&'static str> {
    let host_config = HostConfig {
        publish_all_ports: Some(true),
        ..Default::default()
    };

    container::Config {
        image: Some(POSTGRES_IMAGE),
        env: Some(vec!["POSTGRES_PASSWORD=password", "POSTGRES_USER=postgres"]),
        host_config: Some(host_config),
        ..Default::default()
    }
}

fn build_ipfs_container_config() -> container::Config<&'static str> {
    let host_config = HostConfig {
        publish_all_ports: Some(true),
        ..Default::default()
    };

    container::Config {
        image: Some(IPFS_IMAGE),
        host_config: Some(host_config),
        ..Default::default()
    }
}

fn build_ganache_container_config() -> container::Config<&'static str> {
    let host_config = HostConfig {
        publish_all_ports: Some(true),
        ..Default::default()
    };

    container::Config {
        image: Some(GANACHE_IMAGE),
        cmd: Some(vec![
            "-d",
            "-l",
            "100000000000",
            "-g",
            "1",
            "--noVMErrorsOnRPCResponse",
        ]),
        host_config: Some(host_config),
        ..Default::default()
    }
}

impl TestContainerService {
    fn config(&self) -> container::Config<&'static str> {
        use TestContainerService::*;
        match self {
            Postgres => build_postgres_container(),
            Ipfs => build_ipfs_container_config(),
            Ganache(_u32) => build_ganache_container_config(),
        }
    }

    fn options(&self) -> container::CreateContainerOptions<String> {
        container::CreateContainerOptions { name: self.name() }
    }

    fn name(&self) -> String {
        use TestContainerService::*;
        match self {
            Postgres => "graph_node_integration_test_postgres".into(),
            Ipfs => "graph_node_integration_test_ipfs".into(),
            Ganache(container_count) => {
                format!("graph_node_integration_test_ganache_{}", container_count)
            }
        }
    }
}

/// Handles the connection to the docker daemon and keeps track the service running inside it.
struct DockerTestClient {
    service: TestContainerService,
    client: Docker,
}

impl DockerTestClient {
    async fn start(service: TestContainerService) -> Result<Self, DockerError> {
        println!(
            "Connecting to docker daemon for service: {}",
            service.name()
        );
        let client =
            Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

        // try to remove the container if it already exists
        let _ = client.remove_container(&service.name(), None).await;

        // create docker container
        println!("Creating service container for: {}", service.name());

        client
            .create_container(Some(service.options()), service.config())
            .await?;

        // start docker container
        println!("Starting service container for: {}", service.name());
        client
            .start_container::<&'static str>(&service.name(), None)
            .await?;

        Ok(Self { service, client })
    }

    async fn stop(&self) -> Result<(), DockerError> {
        println!("Stopping service container for: {}", self.service.name());
        self.client
            .kill_container::<String>(&self.service.name(), None)
            .await
    }

    async fn exposed_ports(&self) -> Result<MappedPorts, DockerError> {
        use bollard::models::ContainerSummaryInner;
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), vec![self.service.name()]);
        let options = Some(container::ListContainersOptions {
            filters,
            ..Default::default()
        });
        let results = self.client.list_containers(options).await?;
        if let &[ContainerSummaryInner {
            ports: Some(ports), ..
        }] = &results.as_slice()
        {
            Ok(ports.to_vec().into())
        } else {
            Ok(Vec::new().into())
        }
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

    // Show discovered tests
    println!(
        "Found {} integration test directories:",
        integration_tests_directories.len()
    );
    for dir in &integration_tests_directories {
        println!("  - {}", basename(dir));
    }

    // start docker containers for Postgres and IPFS
    let postgres = DockerTestClient::start(TestContainerService::Postgres)
        .await
        .expect("failed to start container service for Postgres.");
    let ipfs = DockerTestClient::start(TestContainerService::Ipfs)
        .await
        .expect("failed to start container service for IPFS.");

    let service_ports = Arc::new(TestServicePorts {
        postgres: postgres
            .exposed_ports()
            .await
            .expect("failed to obtain exposed ports for the Postgres container"),
        ipfs: ipfs
            .exposed_ports()
            .await
            .expect("failed to obtain exposed ports for the IPFS container"),
    });

    // run tests
    let mut tests_futures = FuturesUnordered::new();
    for dir in integration_tests_directories.into_iter() {
        tests_futures.push(tokio::spawn(run_integration_test(
            dir,
            Arc::clone(&service_ports),
        )));
    }
    while let Some(test_result) = tests_futures.next().await {
        // let test_result = test_result.expect("failed to await for test future.");
        println!("{:?}", test_result);
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

/// Maps `Service => Host` exposed ports.
#[derive(Debug)]
struct MappedPorts(HashMap<u16, u16>);

impl From<Vec<bollard::models::Port>> for MappedPorts {
    fn from(input: Vec<bollard::models::Port>) -> MappedPorts {
        let mut hashmap = HashMap::new();
        let mut empty = true;

        for port in &input {
            if let bollard::models::Port {
                private_port,
                public_port: Some(public_port),
                ..
            } = port
            {
                empty = false;
                hashmap.insert(*private_port as u16, *public_port as u16);
            }
        }
        if empty {
            panic!("Error: container exposed no ports.")
        }
        MappedPorts(hashmap)
    }
}

/// Provides port mappings for Postgres and IPFS service containers.
#[derive(Debug)]
struct TestServicePorts {
    postgres: MappedPorts,
    ipfs: MappedPorts,
}

#[derive(Debug)]
struct TestResult {
    name: String,
    success: bool,
}

async fn run_integration_test(
    test_directory: PathBuf,
    _service_ports: Arc<TestServicePorts>,
) -> TestResult {
    // start a dedicated ganache container for this test
    let test_name = basename(&test_directory);
    let ganache = DockerTestClient::start(TestContainerService::Ganache(get_unique_counter()))
        .await
        .expect("failed to start container service for Ganache.");

    let _ganache_ports = ganache
        .exposed_ports()
        .await
        .expect("failed to obtain exposed ports for Ganache container");

    println!("Test started: {}", basename(&test_directory));

    // discover programs paths
    let _graph_cli = test_directory.join("node_modules/.bin/graph");
    let _graph_node = fs::canonicalize("../target/debug/graph-node")
        .expect("failed to infer `graph-node` program location. (Was it built already?)");

    // run the test
    let success = run_command(vec!["yarn", "test"], &test_directory).await;

    // stop ganache container
    ganache
        .stop()
        .await
        .expect("failed to stop container service for Ganache");

    TestResult {
        name: test_name.to_string(),
        success,
    }
}

/// Prefixes each line with a pipe and a space
fn pretty_output(stdio: &[u8], prefix: &str) -> String {
    String::from_utf8_lossy(&stdio)
        .trim()
        .split("\n")
        .map(|s| format!("{}{}", prefix, s))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Runs a command and prints both its stdout and stderr
async fn run_command(args: Vec<&str>, cwd: &Path) -> bool {
    let command_string = args.clone().join(" ");
    let (program, args) = args.split_first().expect("empty command provided");

    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .await
        .expect(&format!("command failed to run: `{}`", command_string));

    // print stdout and stderr
    let test_name = basename(cwd);
    let stdout_tag = format!("[{}:stdout] ", test_name);
    let stderr_tag = format!("[{}:stderr] ", test_name);
    println!("{}", pretty_output(&output.stdout, &stdout_tag));
    println!("{}", pretty_output(&output.stderr, &stderr_tag));

    output.status.success()
}
