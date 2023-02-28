use futures::StreamExt;
use serde::Serialize;
use sui_sdk::rpc_types::{SuiEventFilter, SuiEventEnvelope};
use sui_sdk::SuiClient;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let sui = SuiClient::new("http://10.0.0.128:9000", Some("ws://10.0.0.128:9000"), None).await?;
    let mut subscribe_all = sui.event_api().subscribe_event(SuiEventFilter::All(vec![])).await?;
    loop {
        let stream_return = subscribe_all.next().await;
        match stream_return {
            Some(sr) => match sr {
                Ok(item) => to_json(item),
                Err(e) => println!("no item"),
            },
            None => println!("error: none"),
    }
        // println!("{:?}", subscribe_all.next().await);
    }

}

fn to_json(item: SuiEventEnvelope) {
    let json_string = serde_json::to_string(&item);
    match json_string {
        Ok(s) => println!("{}", s),
        Err(_e) => println!("cannot serialize to json"),
    }
}