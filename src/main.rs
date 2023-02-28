use futures::StreamExt;
use serde::Serialize;
use sui_sdk::rpc_types::{SuiEventFilter, SuiEventEnvelope};
use sui_sdk::SuiClient;
use std::env;
use anyhow::Result;

use pulsar::{
    authentication::oauth2::OAuth2Authentication, message::proto, producer, Authentication,
    Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};

#[derive(Serialize)]
struct EventJson {
    data: String,
}

impl SerializeMessage for EventJson {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();
    let sui = SuiClient::new("http://10.0.0.128:9000", Some("ws://10.0.0.128:9000"), None).await?;
    let mut subscribe_all = sui.event_api().subscribe_event(SuiEventFilter::All(vec![])).await?;
    loop {
        let stream_return = subscribe_all.next().await;
        match stream_return {
            Some(sr) => match sr {
                Ok(item) => {
                    println!("inside some item match");
                    let _json1 = to_json(item).await;
                },
                Err(_e) => println!("no item"),
            },
            None => println!("error: none"),
    }
        // println!("{:?}", subscribe_all.next().await);
    }

}

async fn to_json(item: SuiEventEnvelope) {
    println!("inside to_json");
    let json_string = serde_json::to_string(&item);
    match json_string {
        Ok(s) => {
            println!("inside string match");
            let _pulsar_return = publish_to_pulsar(s).await;
            // match pulsar_return {
            //     Some(pr) => match pr {
            //         Ok(r) => println!("ok"),
            //         Err(_e) => println!("unable to publish"),
            //     }
            // }
        },
        Err(_e) => println!("cannot serialize to json"),
    }
    
}

async fn publish_to_pulsar(json_item: String) -> Result<(), pulsar::Error> {
    println!("inside publish_to_pulsar");
    println!("{}", json_item);

    let addr = env::var("PULSAR_ADDRESS")
        .ok()
        .unwrap_or_else(|| "pulsar+ssl://sui-devnet.o-mvqin.snio.cloud:6651".to_string());
    let topic = env::var("PULSAR_TOPIC")
        .ok()
        .unwrap_or_else(|| "non-persistent://public/default/all-events".to_string());

    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Ok(token) = env::var("PULSAR_TOKEN") {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };

        builder = builder.with_auth(authentication);
    } else if let Ok(oauth2_cfg) = env::var("PULSAR_OAUTH2") {
        builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
            serde_json::from_str(oauth2_cfg.as_str())
                .unwrap_or_else(|_| panic!("invalid oauth2 config [{}]", oauth2_cfg.as_str())),
        ));
    }

    let pulsar: Pulsar<_> = builder.build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("all-events")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .build()
        .await?;
    producer
        .send(EventJson {
        data: json_item,
        })
        .await?
        .await
        .unwrap();

    Ok(())
}