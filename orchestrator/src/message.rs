use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::worker;

pub type MessageId = u64;
pub type CommandId = u64;
pub type Path = String;
pub type ResponseError = String;

pub type TaggedCoordinatorMessage = (MessageId, CoordinatorMessage);
pub type TaggedWorkerMessage = (MessageId, WorkerMessage);

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub reqs: Vec<Request>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobReport {
    pub resps: Vec<ResponseResult>,
}

impl JobReport {
    pub fn is_ok(&self) -> bool {
        if let Some(resp) = self.resps.last() {
            resp.0.is_ok()
        } else {
            false
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CoordinatorMessage {
    Request(Job),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Response(JobReport),
    StdoutPacket(CommandId, String),
    StderrPacket(CommandId, String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    WriteFile(WriteFileRequest),
    ReadFile(ReadFileRequest),
    ExecuteCommand(ExecuteCommandRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    WriteFile(WriteFileResponse),
    ReadFile(ReadFileResponse),
    ExecuteCommand(ExecuteCommandResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteFileRequest {
    pub path: Path,
    pub content: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteFileResponse(pub ());

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadFileRequest {
    pub path: Path,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadFileResponse(pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteCommandRequest {
    pub cmd: String,
    pub args: Vec<String>,
    pub envs: HashMap<String, String>,
    pub cwd: Option<String>, // None means in project direcotry.
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteCommandResponse(pub ());

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamCommandResponse(pub ());

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseResult(pub Result<Response, ResponseError>);

impl From<Result<Response, worker::Error>> for ResponseResult {
    fn from(value: Result<Response, worker::Error>) -> Self {
        ResponseResult(value.map_err(|e| format!("{e:#}")))
    }
}
