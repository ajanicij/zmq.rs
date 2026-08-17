use zeromq::*;

use std::error::Error;
use std::time::Duration;

#[tokio::test]
async fn subscribe_accepts_binary_prefix() -> Result<(), Box<dyn Error>> {
    let mut publisher = PubSocket::new();
    let endpoint = publisher.bind("tcp://127.0.0.1:0").await?;

    let mut subscriber = SubSocket::new();
    subscriber.connect(&endpoint.to_string()).await?;
    // Non-UTF8 subscription prefix (libzmq allows arbitrary bytes).
    subscriber.subscribe(b"\xfftopic").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(
            b"\xfftopic-one",
        )))
        .await?;
    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(b"other")))
        .await?;
    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(
            b"\xfftopic-two",
        )))
        .await?;

    let first = subscriber.recv().await?;
    assert_eq!(&first.get(0).expect("frame")[..], b"\xfftopic-one");

    // Second matching message proves the non-matching middle message was skipped.
    let third = subscriber.recv().await?;
    assert_eq!(&third.get(0).expect("frame")[..], b"\xfftopic-two");

    Ok(())
}

#[tokio::test]
async fn unsubscribe_accepts_binary_prefix() -> Result<(), Box<dyn Error>> {
    let mut publisher = PubSocket::new();
    let endpoint = publisher.bind("tcp://127.0.0.1:0").await?;

    let mut subscriber = SubSocket::new();
    subscriber.connect(&endpoint.to_string()).await?;
    subscriber.subscribe(b"\x01bin").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(b"\x01bin-1")))
        .await?;
    let msg = subscriber.recv().await?;
    assert_eq!(&msg.get(0).expect("frame")[..], b"\x01bin-1");

    subscriber.unsubscribe(b"\x01bin").await?;
    subscriber.subscribe(b"\x02other").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // After unsubscribe, old-prefix traffic must be skipped; new-prefix arrives.
    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(b"\x01bin-2")))
        .await?;
    publisher
        .send(ZmqMessage::from(bytes::Bytes::from_static(b"\x02other-ok")))
        .await?;

    let msg = subscriber.recv().await?;
    assert_eq!(&msg.get(0).expect("frame")[..], b"\x02other-ok");

    Ok(())
}

#[tokio::test]
async fn string_subscribe_still_works() -> Result<(), Box<dyn Error>> {
    let mut publisher = PubSocket::new();
    let endpoint = publisher.bind("tcp://127.0.0.1:0").await?;

    let mut subscriber = SubSocket::new();
    subscriber.connect(&endpoint.to_string()).await?;
    subscriber.subscribe("news").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    publisher.send("noise".into()).await?;
    publisher.send("news.flash".into()).await?;
    publisher.send("news.update".into()).await?;

    let first = subscriber.recv().await?;
    let first = String::from_utf8(first.get(0).expect("frame").to_vec())?;
    assert_eq!(first, "news.flash");

    let second = subscriber.recv().await?;
    let second = String::from_utf8(second.get(0).expect("frame").to_vec())?;
    assert_eq!(second, "news.update");

    Ok(())
}
