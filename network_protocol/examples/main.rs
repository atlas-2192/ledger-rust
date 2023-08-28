use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
}

async fn receiver(listener: TcpListener) -> TcpStream {
    let listener = listener.accept().await.unwrap().0;
    println!("listener: {}", &listener.peer_addr().unwrap());
    listener
}