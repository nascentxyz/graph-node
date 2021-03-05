mod docker {
    use bollard::models::HostConfig;
    use bollard::{container, Docker};
    use std::collections::HashMap;
    use std::convert::{TryFrom, TryInto};

    const POSTGRES_IMAGE: &'static str = "postgres";
    const IPFS_IMAGE: &'static str = "ipfs/go-ipfs:v0.4.23";
    const GANACHE_IMAGE: &'static str = "trufflesuite/ganache-cli";
    type DockerError = bollard::errors::Error;

    pub async fn stop_and_remove(client: &Docker, service_name: &str) -> Result<(), DockerError> {
        client.kill_container::<&str>(service_name, None).await?;
        client.remove_container(service_name, None).await
    }

    /// Represents all possible service containers to be spawned
    #[derive(Debug)]
    pub enum TestContainerService {
        Postgres,
        Ipfs,
        Ganache(u32),
    }

    impl TestContainerService {
        fn config(&self) -> container::Config<&'static str> {
            use TestContainerService::*;
            match self {
                Postgres => Self::build_postgres_container_config(),
                Ipfs => Self::build_ipfs_container_config(),
                Ganache(_u32) => Self::build_ganache_container_config(),
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

        fn build_postgres_container_config() -> container::Config<&'static str> {
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
    }

    /// Handles the connection to the docker daemon and keeps track the service running inside it.
    pub struct DockerTestClient {
        service: TestContainerService,
        client: Docker,
    }

    impl DockerTestClient {
        pub async fn start(service: TestContainerService) -> Result<Self, DockerError> {
            let client =
                Docker::connect_with_local_defaults().expect("Failed to connect to docker daemon.");

            // try to remove the container if it already exists
            let _ = stop_and_remove(&client, &service.name()).await;

            // create docker container
            client
                .create_container(Some(service.options()), service.config())
                .await?;

            // start docker container
            client
                .start_container::<&'static str>(&service.name(), None)
                .await?;

            Ok(Self { service, client })
        }

        pub async fn stop(&self) -> Result<(), DockerError> {
            println!("Stopping service container for: {}", self.service.name());
            stop_and_remove(&self.client, &self.service.name()).await
        }

        pub async fn exposed_ports(&self) -> Result<MappedPorts, DockerError> {
            use bollard::models::ContainerSummaryInner;
            let mut filters = HashMap::new();
            filters.insert("name".to_string(), vec![self.service.name()]);
            let options = Some(container::ListContainersOptions {
                filters,
                limit: Some(1),
                ..Default::default()
            });
            let results = self.client.list_containers(options).await?;
            match &results.as_slice() {
                &[ContainerSummaryInner {
                    ports: Some(ports), ..
                }] => Ok(ports.to_vec().try_into().unwrap()),
                unexpected_response => panic!(
                    "Received a unexpected_response from docker API: {:#?}",
                    unexpected_response
                ),
            }
        }
    }

    /// Maps `Service => Host` exposed ports.
    #[derive(Debug)]
    pub struct MappedPorts(pub HashMap<u16, u16>);

    impl TryFrom<Vec<bollard::models::Port>> for MappedPorts {
        type Error = &'static str;
        fn try_from(input: Vec<bollard::models::Port>) -> Result<Self, Self::Error> {
            let mut hashmap = HashMap::new();

            for port in &input {
                if let bollard::models::Port {
                    private_port,
                    public_port: Some(public_port),
                    ..
                } = port
                {
                    hashmap.insert(*private_port as u16, *public_port as u16);
                }
            }
            if hashmap.is_empty() {
                Err("Container exposed no ports")
            } else {
                Ok(MappedPorts(hashmap))
            }
        }
    }
}

mod helpers {
    use super::docker::MappedPorts;
    use std::collections::HashSet;
    use std::ffi::OsStr;
    use std::fs;
    use std::io::{self, BufRead};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A counter for uniquely naming Ganache containers
    static GANACHE_CONTAINER_COUNT: AtomicUsize = AtomicUsize::new(0);
    /// A counter for uniquely naming Postgres databases
    static POSTGRES_DATABASE_COUNT: AtomicUsize = AtomicUsize::new(0);
    const POSTGRESQL_DEFAULT_PORT: u16 = 5432;
    const GANACHE_DEFAULT_PORT: u16 = 8545;
    const IPFS_DEFAULT_PORT: u16 = 5001;

    /// Recursivelly find directories that contains a `subgraph.yaml` file.
    pub fn discover_test_directories(dir: &Path, max_depth: u8) -> io::Result<HashSet<PathBuf>> {
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

    /// Strip parent directories from filenames
    pub fn basename(path: &Path) -> String {
        path.file_name()
            .map(OsStr::to_string_lossy)
            .map(String::from)
            .expect("failed to infer basename for path.")
    }

    /// Fetches a unique number for naming Ganache containers
    pub fn get_unique_ganache_counter() -> u32 {
        increase_atomic_counter(&GANACHE_CONTAINER_COUNT)
    }
    /// Fetches a unique number for naming Postgres databases
    pub fn get_unique_postgres_counter() -> u32 {
        increase_atomic_counter(&POSTGRES_DATABASE_COUNT)
    }

    fn increase_atomic_counter(counter: &'static AtomicUsize) -> u32 {
        let old_count = counter.fetch_add(1, Ordering::SeqCst);
        old_count as u32 + 1
    }

    /// Parses stdio bytes into a prefixed String
    pub fn pretty_output(stdio: &[u8], prefix: &str) -> String {
        let mut cursor = io::Cursor::new(stdio);
        let mut buf = vec![];
        let mut string = String::new();
        loop {
            buf.clear();
            let bytes_read = cursor
                .read_until(b'\n', &mut buf)
                .expect("failed to read from stdio.");
            if bytes_read == 0 {
                break;
            }
            let as_string = String::from_utf8_lossy(&buf);
            string.push_str(&prefix);
            string.push_str(&as_string); // will contain a newline
        }
        string
    }

    /// Returns five available port numbers
    pub fn get_five_ports() -> [u16; 5] {
        let mut ports = [0_u16; 5];
        for port in ports.iter_mut() {
            *port = get_port::get_port().expect("failed to obtain a free port")
        }
        ports
    }

    // Build a postgres connection string
    pub fn make_postgres_uri(unique_id: u32, postgres_ports: &MappedPorts) -> String {
        let port = postgres_ports
            .0
            .get(&POSTGRESQL_DEFAULT_PORT)
            .expect("failed to fetch Postgres port from mapped ports");
        format!(
            "postgresql://{user}:{password}@{host}:{port}/{database_name}",
            user = "postgres",
            password = "postgres",
            host = "localhost",
            port = port,
            database_name = format!("test_database_{}", unique_id),
        )
    }

    pub fn make_ipfs_uri(ipfs_ports: &MappedPorts) -> String {
        let port = ipfs_ports
            .0
            .get(&IPFS_DEFAULT_PORT)
            .expect("failed to fetch IPFS port from mapped ports");
        format!("{host}:{port}", host = "localhost", port = port)
    }

    // Build a Ganache connection string
    pub fn make_ganache_uri(ganache_ports: &MappedPorts) -> String {
        let port = ganache_ports
            .0
            .get(&GANACHE_DEFAULT_PORT)
            .expect("failed to fetch Ganache port from mapped ports");
        format!(
            "test://http://{host}:{port}",
            host = "localhost",
            port = port
        )
    }
}

mod integration_testing {
    use super::docker::{DockerTestClient, MappedPorts, TestContainerService};
    use super::helpers::{
        basename, discover_test_directories, get_five_ports, get_unique_ganache_counter,
        get_unique_postgres_counter, make_ganache_uri, make_ipfs_uri, make_postgres_uri,
        pretty_output,
    };
    use futures::{stream::futures_unordered::FuturesUnordered, StreamExt};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::process::Command;

    /// Contains all information a test command needs
    #[derive(Debug)]
    struct IntegrationTestSetup {
        postgres_uri: String,
        ipfs_uri: String,
        ganache_uri: String,
        graph_node_ports: [u16; 5],
        graph_node_bin: PathBuf,
        graph_cli_bin: PathBuf,
        test_directory: PathBuf,
    }

    impl IntegrationTestSetup {
        fn test_name(&self) -> String {
            basename(&self.test_directory)
        }
    }

    /// Info about a finished test command
    #[derive(Debug)]
    struct TestCommandResults {
        success: bool,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    }

    // The results of a finished integration test
    #[derive(Debug)]
    struct IntegrationTestResult {
        test_setup: IntegrationTestSetup,
        command_results: TestCommandResults,
    }

    /// The main test entrypoint
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

        let postgres_ports = Arc::new(
            postgres
                .exposed_ports()
                .await
                .expect("failed to obtain exposed ports for the Postgres container"),
        );
        let ipfs_ports = Arc::new(
            ipfs.exposed_ports()
                .await
                .expect("failed to obtain exposed ports for the IPFS container"),
        );

        // run tests
        let mut test_results = Vec::new();
        let mut exit_code: i32 = 0;
        let mut tests_futures = FuturesUnordered::new();
        for dir in integration_tests_directories.into_iter() {
            tests_futures.push(tokio::spawn(run_integration_test(
                dir,
                Arc::clone(&postgres_ports),
                Arc::clone(&ipfs_ports),
            )));
        }
        while let Some(test_result) = tests_futures.next().await {
            let test_result = test_result.expect("failed to await for test future.");
            if !test_result.command_results.success {
                exit_code = 101;
            }
            test_results.push(test_result);
        }

        // Stop containers.
        postgres
            .stop()
            .await
            .expect("failed to stop container service for Postgres");
        ipfs.stop()
            .await
            .expect("failed to stop container service for IPFS");

        // print test results
        println!("\nTest results:");
        for test_result in &test_results {
            println!("- {:?}", test_result)
        }

        std::process::exit(exit_code)
    }

    /// Prepare and run the integration test
    async fn run_integration_test(
        test_directory: PathBuf,
        postgres_ports: Arc<MappedPorts>,
        ipfs_ports: Arc<MappedPorts>,
    ) -> IntegrationTestResult {
        // start a dedicated ganache container for this test
        let unique_ganache_counter = get_unique_ganache_counter();
        let ganache =
            DockerTestClient::start(TestContainerService::Ganache(unique_ganache_counter))
                .await
                .expect("failed to start container service for Ganache.");

        let ganache_ports = ganache
            .exposed_ports()
            .await
            .expect("failed to obtain exposed ports for Ganache container");

        // discover programs paths
        let graph_cli = fs::canonicalize(test_directory.join("node_modules/.bin/graph"))
            .expect("failed to infer `@graphprotocol/graph-cli` program location.");
        let graph_node = fs::canonicalize("../target/debug/graph-node")
            .expect("failed to infer `graph-node` program location. (Was it built already?)");

        // build URIs
        let postgres_uri = make_postgres_uri(get_unique_postgres_counter(), &postgres_ports);
        let ipfs_uri = make_ipfs_uri(&ipfs_ports);
        let ganache_uri = make_ganache_uri(&ganache_ports);

        // run test comand
        let test_setup = IntegrationTestSetup {
            postgres_uri,
            ipfs_uri,
            ganache_uri,
            graph_node_ports: get_five_ports(),
            graph_node_bin: graph_node,
            graph_cli_bin: graph_cli,
            test_directory,
        };
        println!("Test started: {}", basename(&test_setup.test_directory));
        let command_results = run_test_command(&test_setup).await;

        // stop ganache container
        ganache
            .stop()
            .await
            .expect("failed to stop container service for Ganache");

        IntegrationTestResult {
            test_setup,
            command_results,
        }
    }

    /// Runs a command for a integration test
    async fn run_test_command(test_setup: &IntegrationTestSetup) -> TestCommandResults {
        let output = Command::new("yarn")
            .arg("test")
            .env("GANACHE_TEST_PORT", test_setup.ganache_port.to_string())
            .current_dir(&test_setup.test_directory)
            .output()
            .await
            .expect("failed to run test command");

        let test_name = test_setup.test_name();
        let stdout_tag = format!("[{}:stdout]", test_name);
        let stderr_tag = format!("[{}:stderr]", test_name);

        TestCommandResults {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: pretty_output(&output.stdout, &stdout_tag),
            stderr: pretty_output(&output.stderr, &stderr_tag),
        }
    }
}
