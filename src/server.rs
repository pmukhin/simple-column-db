use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use quinn::{Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::Certificate;

pub async fn create_endpoint(addr: &SocketAddr) -> Result<Endpoint, Box<dyn Error>> {
    // Generate a self-signed TLS certificate
    let cert = generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.serialize_der()?;
    let key_der = cert.serialize_private_key_der();

    // Configure TLS
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(vec![Certificate(cert_der)], rustls::PrivateKey(key_der))?;

    server_crypto.alpn_protocols = vec![b"h3".to_vec()]; // HTTP/3 ALPN identifier

    // Configure QUIC server
    let server_config = ServerConfig::with_crypto(Arc::new(server_crypto));
    // let addr: SocketAddr = "127.0.0.1:4433".parse()?;
    let e = Endpoint::server(server_config, addr.clone())?;

    Ok(e)
}

