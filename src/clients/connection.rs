use std::net::TcpStream;
use std::process::exit;




pub fn connect_to_server(host: &str) -> TcpStream {
    match TcpStream::connect(host) {
        Ok(stream) => {
            println!("Successfully connected to Kafka cluster on hosts: {}", host);
            stream
        }
        Err(e) => {
            println!("Failed to connect: {}", e);
            exit(1);
        }
    }
}




