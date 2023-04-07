use snafu::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    convert::{TryFrom, TryInto},
    path::{Path, PathBuf},
    process::Stdio,
    str::from_utf8,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::{
    join,
    process::{Child, ChildStdin, ChildStdout, Command},
    select,
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::io::SyncIoBridge;

use crate::{
    message::{
        CommandId, CoordinatorMessage, Job, JobReport, MessageId, TaggedCoordinatorMessage,
        TaggedWorkerMessage, WorkerMessage,
    },
    sandbox::{CompileRequest, CompileResponse},
};

#[derive(Debug)]
enum FanoutCommand {
    Listen(u64, mpsc::Sender<ContainerMessage>),
}

type TaggedContainerMessage = (MessageId, ContainerMessage);
type TaggedPlaygroundMessage = (MessageId, PlaygroundMessage);
type TaggedRequestKind = (MessageId, RequestKind);

#[derive(Debug)]
pub struct Container {
    child: Child,
    tx: mpsc::Sender<TaggedPlaygroundMessage>,
    command_tx: mpsc::Sender<FanoutCommand>,
    id: AtomicU64,
}

impl Container {
    async fn fanout(
        mut command_rx: mpsc::Receiver<FanoutCommand>,
        mut rx: mpsc::Receiver<TaggedContainerMessage>,
    ) {
        let mut waiting = HashMap::new();

        loop {
            select! {
                command = command_rx.recv() => {
                    let command = command.expect("Handle this");
                    match command {
                        FanoutCommand::Listen(id, waiter) => {
                            waiting.insert(id, waiter);
                            // TODO: ensure not replacing
                        }
                    }
                },

                msg = rx.recv() => {
                    let (id, msg) = msg.expect("Handle this");
                    if let Some(waiter) = waiting.get(&id) {
                        waiter.send(msg).await.ok(/* Don't care about it */);
                    }
                    // TODO: log unattended messages?
                }
            }
        }
    }

    pub async fn compile(&self, request: CompileRequest) -> Result<CompileResponse> {
        let (result, stdout, stderr) = self.begin_compile(request).await?;

        let stdout = ReceiverStream::new(stdout).collect();
        let stderr = ReceiverStream::new(stderr).collect();

        let (result, stdout, stderr) = join!(result, stdout, stderr);

        let mut result = result.expect("handle me")?;
        result.stdout = stdout;
        result.stderr = stderr;

        Ok(result)
    }

    pub async fn begin_compile(
        &self,
        request: CompileRequest,
    ) -> Result<(
        JoinHandle<Result<CompileResponse>>, // TODO: shouldn't include stdout / stderr
        mpsc::Receiver<String>,
        mpsc::Receiver<String>,
    )> {
        let Self {
            tx: to_worker_tx,
            command_tx,
            id,
            ..
        } = self;
        let id = id.fetch_add(1, Ordering::SeqCst);

        let (from_worker_tx, mut from_worker_rx) = mpsc::channel(8);

        command_tx
            .send(FanoutCommand::Listen(id, from_worker_tx))
            .await
            .unwrap();
        to_worker_tx
            .send((
                id,
                PlaygroundMessage::Request(HighLevelRequest::Compile(request)),
            ))
            .await
            .expect("handle this");

        let (stdout_tx, stdout_rx) = mpsc::channel(8);
        let (stderr_tx, stderr_rx) = mpsc::channel(8);

        let x = tokio::spawn(async move {
            while let Some(container_msg) = from_worker_rx.recv().await {
                match container_msg {
                    ContainerMessage::Response(resp) => match resp {
                        HighLevelResponse::Compile(resp) => return Ok(resp),
                    },
                    ContainerMessage::StdoutPacket(_, packet) => {
                        stdout_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                    }
                    ContainerMessage::StderrPacket(_, packet) => {
                        stderr_tx.send(packet).await.ok(/* Receiver gone, that's OK */);
                    }
                }
            }

            // TODO: Stop listening

            panic!("Shouldn't have ended yet");
        });

        Ok((x, stdout_rx, stderr_rx))
    }
}

#[derive(Debug)]
pub enum PlaygroundMessage {
    Request(HighLevelRequest),
}

#[derive(Debug)]
pub enum ContainerMessage {
    Response(HighLevelResponse),
    StdoutPacket(CommandId, String),
    StderrPacket(CommandId, String),
}

#[derive(Debug)]
pub enum HighLevelRequest {
    Compile(CompileRequest),
}

#[derive(Debug)]
pub enum HighLevelResponse {
    Compile(CompileResponse),
}

#[derive(Debug)]
pub struct Coordinator {
    free_containers: VecDeque<Container>,
    worker_project_dir: PathBuf,
}

impl Coordinator {
    pub fn new(project_dir: &Path) -> Self {
        Coordinator {
            free_containers: VecDeque::new(),
            worker_project_dir: project_dir.to_path_buf(),
        }
    }

    pub fn allocate(&mut self) -> Result<Container, Error> {
        if let Some(container) = self.free_containers.pop_front() {
            Ok(container)
        } else {
            spawn_container(self.worker_project_dir.as_path())
        }
    }
}

pub type Result<T, E = Error> = ::std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Reached system process limit"))]
    SpawnWorker { source: std::io::Error },

    #[snafu(display("Worker process's stdin not captured"))]
    WorkerStdinCapture,

    #[snafu(display("Worker process's stdout not captured"))]
    WorkerStdoutCapture,

    #[snafu(display("Failed to flush child stdin"))]
    WorkerStdinFlush { source: std::io::Error },

    #[snafu(display("Failed to deserialize worker message"))]
    WorkerMessageDeserialization { source: bincode::Error },

    #[snafu(display("Failed to serialize coordinator message"))]
    CoordinatorMessageSerialization { source: bincode::Error },

    #[snafu(display("Failed to send worker message through channel"))]
    UnableToSendWorkerMessage {
        source: mpsc::error::SendError<TaggedWorkerMessage>,
    },

    #[snafu(display("Failed to receive worker message through channel"))]
    UnableToReceiveWorkerMessage,

    #[snafu(display("Failed to send coordinator message through channel"))]
    UnableToSendCoordinatorMessage {
        source: mpsc::error::SendError<CoordinatorMessage>,
    },

    #[snafu(display("Failed to receive coordinator message through channel"))]
    UnableToReceiveCoordinatorMessage,

    #[snafu(display("Failed to send worker response(job report) through channel"))]
    UnableToSendJobReport,

    #[snafu(display("Failed to send request kind"))]
    UnableToSendRequestKind {
        source: mpsc::error::SendError<TaggedRequestKind>,
    },

    #[snafu(display("Failed to send job"))]
    UnableToSendJob {
        source: mpsc::error::SendError<TaggedCoordinatorMessage>,
    },

    #[snafu(display("Failed to send container respones"))]
    UnableToSendContainerResponse {
        source: mpsc::error::SendError<TaggedContainerMessage>,
    },

    #[snafu(display("Failed to send stdout packet"))]
    UnableToSendStdoutPacket {
        source: mpsc::error::SendError<TaggedContainerMessage>,
    },

    #[snafu(display("Failed to send stderr packet"))]
    UnableToSendStderrPacket {
        source: mpsc::error::SendError<TaggedContainerMessage>,
    },

    #[snafu(display("PlaygroundMessage receiver ended unexpectedly"))]
    PlaygroundMessageReceiverEnded,

    #[snafu(display("WorkerMessage receiver ended unexpectedly"))]
    WorkerMessageReceiverEnded,

    #[snafu(display("RequestKind receiver ended unexpectedly"))]
    RequestKindReceiverEnded,
}

impl TryFrom<HighLevelRequest> for Job {
    type Error = Error;

    fn try_from(value: HighLevelRequest) -> Result<Self, Self::Error> {
        match value {
            HighLevelRequest::Compile(req) => {
                use crate::message::*;
                use crate::sandbox::CompileTarget::*;
                use crate::sandbox::*;

                let mut batch = Vec::new();
                batch.push(Request::WriteFile(WriteFileRequest {
                    path: "src/main.rs".to_owned(),
                    content: req.code.into(),
                }));
                let edition = match req.edition {
                    Some(Edition::Rust2021) => "2021",
                    Some(Edition::Rust2018) => "2018",
                    Some(Edition::Rust2015) => "2015",
                    None => "2021",
                };
                batch.push(Request::WriteFile(WriteFileRequest {
                    path: "Cargo.toml".to_owned(),
                    content: format!(
                        r#"[package]
                                name = "play"
                                version = "0.1.0"
                                edition = "{edition}"
                                "#
                    )
                    .into(),
                }));
                let mut args = if let Wasm = req.target {
                    vec!["wasm", "build"]
                } else {
                    vec!["rustc"]
                };
                if let Mode::Release = req.mode {
                    args.push("--release");
                }
                let output_path: &str = "compilation";
                match req.target {
                    Assembly(flavor, _, _) => {
                        use crate::sandbox::AssemblyFlavor::*;

                        // TODO: No compile-time string formatting.
                        args.extend(&["--", "--emit", "asm=compilation"]);

                        // Enable extra assembly comments for nightly builds
                        if let Channel::Nightly = req.channel {
                            args.push("-Z");
                            args.push("asm-comments");
                        }

                        args.push("-C");
                        match flavor {
                            Att => args.push("llvm-args=-x86-asm-syntax=att"),
                            Intel => args.push("llvm-args=-x86-asm-syntax=intel"),
                        }
                    }
                    LlvmIr => args.extend(&["--", "--emit", "llvm-ir=compilation"]),
                    Mir => args.extend(&["--", "--emit", "mir=compilation"]),
                    Hir => args.extend(&["--", "-Zunpretty=hir", "-o", output_path]),
                    Wasm => args.extend(&["-o", output_path]),
                }
                let mut envs = HashMap::new();
                if req.backtrace {
                    envs.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
                }
                batch.push(Request::ExecuteCommand(ExecuteCommandRequest {
                    cmd: "cargo".to_owned(),
                    args: args.into_iter().map(|s| s.to_owned()).collect(),
                    envs,
                    cwd: None,
                }));
                batch.push(Request::ReadFile(ReadFileRequest {
                    path: output_path.to_owned(),
                }));
                Ok(Job { reqs: batch })
            }
        }
    }
}

#[derive(Debug)]
pub enum RequestKind {
    Compile,
}

impl RequestKind {
    fn kind(req: &HighLevelRequest) -> Self {
        match req {
            HighLevelRequest::Compile(_) => RequestKind::Compile,
        }
    }
}

impl From<(JobReport, RequestKind)> for HighLevelResponse {
    fn from(value: (JobReport, RequestKind)) -> Self {
        use crate::message::*;

        let (JobReport { resps }, kind) = value;
        let responses = resps;
        match kind {
            RequestKind::Compile => {
                let mut success = false;
                let mut code = String::new();
                if let Some(ResponseResult(Ok(Response::ReadFile(ReadFileResponse(content))))) =
                    responses.last()
                {
                    match from_utf8(content) {
                        Ok(content) => {
                            success = true;
                            code = content.to_owned();
                        }
                        Err(_) => {
                            success = false;
                            code = "Error: Compilation output is not valid UTF-8 string".to_owned();
                        }
                    }
                }
                HighLevelResponse::Compile(CompileResponse {
                    success,
                    code,
                    stdout: "".to_owned(),
                    stderr: "".to_owned(),
                })
            }
        }
    }
}

macro_rules! docker_command {
    ($($arg:expr),* $(,)?) => ({
        let mut cmd = Command::new("docker");
        $( cmd.arg($arg); )*
        cmd
    });
}

fn basic_secure_docker_command() -> Command {
    docker_command!(
        "run",
        "--platform",
        "linux/amd64",
        "--cap-drop=ALL",
        "--net",
        "none",
        "--memory",
        "512m",
        "--memory-swap",
        "640m",
        "--pids-limit",
        "512",
    )
}

fn run_worker_in_background(project_dir: &Path) -> Result<(Child, ChildStdin, ChildStdout)> {
    // For local development.
    const WORKER_FILEPATH: &str = "./target/debug/worker";

    let mut child = Command::new(WORKER_FILEPATH)
        // let mut child = basic_secure_docker_command()
        // .arg("-i")
        // .args(["-a", "stdin", "-a", "stdout", "-a", "stderr"])
        // .arg("adwinw/rust-playground-worker")
        .arg(project_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .context(SpawnWorkerSnafu)?;
    let stdin = child.stdin.take().context(WorkerStdinCaptureSnafu)?;
    let stdout = child.stdout.take().context(WorkerStdoutCaptureSnafu)?;
    Ok((child, stdin, stdout))
}

// Child stdin/out <--> messages.
fn spawn_io_queue(
    tasks: &mut JoinSet<Result<()>>,
    stdin: ChildStdin,
    stdout: ChildStdout,
) -> (
    mpsc::Sender<TaggedCoordinatorMessage>,
    mpsc::Receiver<TaggedWorkerMessage>,
) {
    use std::io::{prelude::*, BufReader, BufWriter};

    let (tx, worker_msg_rx) = mpsc::channel(8);
    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdout = SyncIoBridge::new(stdout);
            let mut stdout = BufReader::new(stdout);

            loop {
                let worker_msg = bincode::deserialize_from(&mut stdout)
                    .context(WorkerMessageDeserializationSnafu)?;

                tx.blocking_send(worker_msg)
                    .context(UnableToSendWorkerMessageSnafu)?;
            }
        })
        .await
        .unwrap(/* Panic occurred; re-raising */)
    });

    let (coordinator_msg_tx, mut rx) = mpsc::channel(8);
    tasks.spawn(async move {
        tokio::task::spawn_blocking(move || {
            let stdin = SyncIoBridge::new(stdin);
            let mut stdin = BufWriter::new(stdin);

            loop {
                let coordinator_msg = rx
                    .blocking_recv()
                    .context(UnableToReceiveCoordinatorMessageSnafu)?;

                bincode::serialize_into(&mut stdin, &coordinator_msg)
                    .context(CoordinatorMessageSerializationSnafu)?;

                stdin.flush().context(WorkerStdinFlushSnafu)?;
            }
        })
        .await
        .unwrap(/* Panic occurred; re-raising */)
    });
    (coordinator_msg_tx, worker_msg_rx)
}

pub fn spawn_container(project_dir: &Path) -> Result<Container> {
    let mut tasks = JoinSet::new();
    let (child, stdin, stdout) = run_worker_in_background(project_dir)?;
    let (coordinator_msg_tx, worker_msg_rx) = spawn_io_queue(&mut tasks, stdin, stdout);
    let (playground_msg_tx, playground_msg_rx) = mpsc::channel(8);
    let (container_msg_tx, container_msg_rx) = mpsc::channel(8);
    let (kind_tx, kind_rx) = mpsc::channel(8);
    tasks.spawn(lower_operations(
        playground_msg_rx,
        coordinator_msg_tx,
        kind_tx,
    ));
    tasks.spawn(lift_operation_results(
        worker_msg_rx,
        container_msg_tx,
        kind_rx,
    ));
    tokio::spawn(async move {
        if let Some(task) = tasks.join_next().await {
            eprintln!("{task:?}");

            tasks.abort_all();
            while let Some(task) = tasks.join_next().await {
                eprintln!("{task:?}");
            }
        }
    });

    let (command_tx, command_rx) = mpsc::channel(8);
    tokio::spawn(Container::fanout(command_rx, container_msg_rx));

    Ok(Container {
        child,
        tx: playground_msg_tx,
        command_tx,
        id: AtomicU64::new(0),
    })
}

async fn lower_operations(
    mut playground_msg_rx: mpsc::Receiver<TaggedPlaygroundMessage>,
    coordinator_msg_tx: mpsc::Sender<TaggedCoordinatorMessage>,
    kind_tx: mpsc::Sender<TaggedRequestKind>,
) -> Result<()> {
    loop {
        let (id, playground_msg) = playground_msg_rx
            .recv()
            .await
            .context(PlaygroundMessageReceiverEndedSnafu)?;
        match playground_msg {
            PlaygroundMessage::Request(req) => {
                let kind = RequestKind::kind(&req);
                kind_tx
                    .send((id, kind))
                    .await
                    .context(UnableToSendRequestKindSnafu)?;
                let job = req.try_into()?;
                let coordinator_msg = CoordinatorMessage::Request(job);
                coordinator_msg_tx
                    .send((id, coordinator_msg))
                    .await
                    .context(UnableToSendJobSnafu)?;
            }
        }
    }
}

async fn lift_operation_results(
    mut worker_msg_rx: mpsc::Receiver<TaggedWorkerMessage>,
    container_msg_tx: mpsc::Sender<TaggedContainerMessage>,
    mut kind_rx: mpsc::Receiver<TaggedRequestKind>,
) -> Result<()> {
    let mut request_kinds = HashMap::new();
    loop {
        select! {
            worker_msg = worker_msg_rx.recv() => {
                let (id, worker_msg) = worker_msg.context(WorkerMessageReceiverEndedSnafu)?;
                match worker_msg {
                    WorkerMessage::Response(job_report) => {
                        if let Some(kind) = request_kinds.remove(&id) {
                            let response = (job_report, kind).into();
                            let container_msg = ContainerMessage::Response(response);
                            container_msg_tx.send((id, container_msg)).await.context(UnableToSendContainerResponseSnafu)?;
                        }
                    }
                    WorkerMessage::StdoutPacket(cmd_id, data) => {
                        container_msg_tx.send((id, ContainerMessage::StdoutPacket(cmd_id, data))).await.context(UnableToSendStdoutPacketSnafu)?;
                    }
                    WorkerMessage::StderrPacket(cmd_id, data) => {
                        container_msg_tx.send((id, ContainerMessage::StderrPacket(cmd_id, data))).await.context(UnableToSendStderrPacketSnafu)?;
                    }
                }
            }
            kind_msg = kind_rx.recv() => {
                let (id, kind) = kind_msg.context(RequestKindReceiverEndedSnafu)?;
                request_kinds.insert(id, kind);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tempdir::TempDir;
    use tokio::join;
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};

    use crate::{
        coordinator::Coordinator,
        sandbox::{Channel, CompileRequest, CompileTarget, CrateType, Edition, Mode},
    };

    fn new_compile_request() -> CompileRequest {
        CompileRequest {
            target: CompileTarget::Mir,
            channel: Channel::Stable,
            crate_type: CrateType::Binary,
            mode: Mode::Release,
            edition: Some(Edition::Rust2021),
            tests: false,
            backtrace: false,
            code: r#"fn main() { println!("Hello World!"); }"#.to_owned(),
        }
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_response() -> super::Result<()> {
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let response = tokio::time::timeout(
            Duration::from_millis(5000),
            container.compile(new_compile_request()),
        )
        .await
        .expect("Failed to receive streaming from container in time")?;

        assert!(response.success);

        Ok(())
    }

    #[tokio::test]
    #[snafu::report]
    async fn test_compile_streaming() -> super::Result<()> {
        let project_dir =
            TempDir::new("playground").expect("Failed to create temporary project directory");
        let mut coordinator = Coordinator::new(project_dir.path());

        let container = coordinator.allocate()?;
        let (complete, stdout, stderr) = container.begin_compile(new_compile_request()).await?;

        let stdout = ReceiverStream::new(stdout);
        let stdout = stdout.collect::<String>();

        let stderr = ReceiverStream::new(stderr);
        let stderr = stderr.collect::<String>();

        let (_complete, _stdout, stderr) =
            tokio::time::timeout(Duration::from_millis(5000), async move {
                join!(complete, stdout, stderr)
            })
            .await
            .expect("Failed to receive streaming from container in time");

        assert!(stderr.contains("Compiling"));
        assert!(stderr.contains("Finished"));

        Ok(())
    }
}
