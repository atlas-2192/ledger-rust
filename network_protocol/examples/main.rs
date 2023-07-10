use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:1234").await.unwrap();
    let receiver = receiver(listener).await;
    let res = network::receive_event_async(&receiver).await;
    if res.is_ok() {
        println!("ok")
    }
}

async fn receiver(listener: TcpListener) -> TcpStream {
    let listener = listener.accept().await.unwrap().0;
    println!("listener: {}", &listener.peer_addr().unwrap());
    listener
}