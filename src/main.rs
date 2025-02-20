mod sstable;
mod data;
mod table;
mod cmd;
mod server;
use crate::cmd::Command;
use crate::data::{Data, Schema};
use crate::table::Table;
use quinn::Connecting;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // simple counter
    let mut table = Table::new("default_table".to_string(), vec![
        Schema::String(12),
        Schema::Integer,
    ]);

    let mut rc: Arc<RwLock<Table>> = Arc::new(RwLock::new(table));

    let addr: SocketAddr = "127.0.0.1:4433".parse()?;
    let mut endpoint = server::create_endpoint(&addr).await?;

    println!("🚀 QUIC server running on {}", addr);

    // Handle incoming connections
    while let Some(connecting) = endpoint.accept().await {
        tokio::spawn(handle_connection(connecting, rc.clone()));
    }

    println!("we never exit");
    Ok(())
}

async fn handle_connection(connecting: Connecting, rc: Arc<RwLock<Table>>) {
    match connecting.await {
        Ok(connection) => {
            println!("✅ New connection: {:?}", connection.remote_address());

            loop {
                match connection.accept_bi().await {
                    Ok(stream) => {
                        tokio::spawn(handle_stream(stream, rc.clone()));
                    }
                    Err(e) => {
                        eprintln!("❌ Connection error: {}", e);
                        return;
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("❌ Connection error: {}", e);
            return;
        }
    }
}

async fn handle_stream(
    (mut send, mut recv): (quinn::SendStream, quinn::RecvStream),
    rc: Arc<RwLock<Table>>,
) {
    let mut buffer = [0; 1024];

    if let Ok(Some(n)) = recv.read(&mut buffer).await {
        let data = &buffer[..n];

        match Command::parse(&String::from_utf8_lossy(data)) {
            Ok(Command::Select { .. }) => {
                let g = rc.read().await;
                let data = g.read_all(20);
                let q_message = format!("results: {:?}", data);
                let _ = send.write_all(q_message.as_bytes()).await;
            }
            Ok(q @ Command::Update { .. }) => {
                let q_message = format!("query: {:?}", q);
                let _ = send.write_all(q_message.as_bytes()).await;
            }
            Ok(Command::Insert { name, columns, values }) => {
                let mut g = rc.write().await;
                match &values[0] {
                    Data::String(k) => {
                        let result = g.insert(k.clone(), &values[..]);
                        match result {
                            Ok(_) => {
                                let _ = send.write_all("insert query: OK".as_bytes()).await;
                            }
                            Err(e) => {
                                let q_message = format!("insert query: {:?}", &e);
                                let _ = send.write_all(q_message.as_bytes()).await;
                            }
                        }
                    }
                    _ => panic!("not implemented"),
                }
            }
            Ok(q @ Command::CreateTable { .. }) => {
                let q_message = format!("query: {:?}", q);
                let _ = send.write_all(q_message.as_bytes()).await;
            }
            Err(e) => {
                let e_msg = format!("unexpected query: {:?}", e);
                let _ = send.write_all(e_msg.as_bytes()).await;
            }
        }
    }
}
