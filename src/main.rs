use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fs;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::path::Path;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::stream::StreamExt;
use libp2p::{
    gossipsub, identify, kad, mdns, noise,
    swarm::NetworkBehaviour, swarm::SwarmEvent,
    tcp, yamux, Multiaddr
};

use tokio::io::{self, AsyncBufReadExt};
use tokio::sync::broadcast;

use notify::{Watcher, RecursiveMode, Event, EventKind};
use diffy::{create_patch, apply, Patch};

use anyhow::Result;
use clap::Parser;



#[derive(Parser)]
struct Args {
    #[arg(long)]
    file_path: String,
    #[arg(long, value_parser = clap::value_parser!(Multiaddr))]
    bootstrap_addr: Option<Multiaddr>,
}

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::tokio::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    identify: identify::Behaviour,
    stream: libp2p_stream::Behaviour,
}

const FILE_SYNC_PROTOCOL: libp2p::StreamProtocol = libp2p::StreamProtocol::new("/p2p-chat/file-sync/1.0.0");
const MAX_MSG_LEN: usize = 20_000_000; // sanity cap so a garbage length prefix can't OOM us

const MSG_TYPE_FULL: u8 = 0;
const MSG_TYPE_PATCH: u8 = 1;

/// Write a framed message: [1-byte type][4-byte big-endian len][payload].
async fn write_framed<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg_type: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&[msg_type]).await?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

/// Read one framed message: returns (type, payload bytes).
async fn read_framed<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_buf = [0u8; 1];
    stream.read_exact(&mut type_buf).await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_MSG_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("declared message length {len} exceeds max {MAX_MSG_LEN}"),
        ));
    }

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;

    Ok((type_buf[0], payload))
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let bootstrap_addr: Option<Multiaddr> = args.bootstrap_addr.clone();

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();

            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10))
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

            let kad_config = kad::Config::new(
                libp2p::StreamProtocol::new("/p2p-chat/kad/1.0.0")
            );
            let store = kad::store::MemoryStore::new(local_peer_id);
            let mut kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);
            kademlia.set_mode(Some(kad::Mode::Server));

            let identify = identify::Behaviour::new(identify::Config::new(
                "/p2p-chat/id/1.0.0".to_string(),
                key.public(),
            ));

            let stream = libp2p_stream::Behaviour::new();

            Ok(MyBehaviour { gossipsub, mdns, kademlia, identify, stream })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let topic = gossipsub::IdentTopic::new("chat-room");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/0".parse()?)?;

    if let Some(addr) = bootstrap_addr {
        if let Some(libp2p::multiaddr::Protocol::P2p(peer_id)) = addr.iter().last() {
            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
            swarm.dial(addr)?;
            println!("Dialing bootstrap peer...");
        } else {
            eprintln!("Bootstrap multiaddr must end in /p2p/<peer-id>");
        }
    }

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    println!("Local peer id: {}", swarm.local_peer_id());
    println!("Type a message and press enter to broadcast it to any peers found.\n");

    let mut bootstrap_interval = tokio::time::interval(Duration::from_secs(30));

    let mut control = swarm.behaviour().stream.new_control();
    let mut file_sync_stream = control.accept(FILE_SYNC_PROTOCOL)?;

    let file_path_target = args.file_path.clone();

    // Ensure the target file exists and is readable before we start; if this
    // fails, everything downstream is pointless, so bail out clearly here.
    let initial_content = fs::read_to_string(&file_path_target).map_err(|e| {
        eprintln!("Failed to read initial content of {file_path_target}: {e:?}");
        e
    })?;

    // Single source of truth for "the last content we know is in sync",
    // shared between the receiver task and the watcher thread. Comparing
    // against this (rather than a boolean flag) avoids races where a
    // coalesced or reordered filesystem event causes a remote-applied write
    // to be mistaken for a local edit (or vice versa).
    let last_known_content: Arc<Mutex<String>> = Arc::new(Mutex::new(initial_content));

    let last_known_content_recv = last_known_content.clone();

    // --- Receiver task: accepts incoming file-sync streams, handles full
    //     syncs and patches ---
    tokio::spawn(async move {
        while let Some((peer, mut stream)) = file_sync_stream.next().await {
            println!("[{peer}] file-sync stream opened");

            'stream_loop: loop {
                let (msg_type, payload) = match read_framed(&mut stream).await {
                    Ok(v) => v,
                    Err(e) => {
                        println!("[{peer}] stream closed or read error: {e:?}");
                        break 'stream_loop;
                    }
                };

                match msg_type {
                    MSG_TYPE_FULL => {
                        let content = match String::from_utf8(payload) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[{peer}] full-sync payload was not valid UTF-8: {e:?}");
                                continue 'stream_loop;
                            }
                        };

                        println!("[{peer}] received full sync ({} bytes)", content.len());

                        {
                            let mut guard = last_known_content_recv.lock().unwrap();
                            *guard = content.clone();
                        }

                        if let Err(e) = fs::write(&file_path_target, &content) {
                            eprintln!("[{peer}] failed to write full sync to {file_path_target}: {e:?}");
                            continue 'stream_loop;
                        }

                        println!("[{peer}] applied full sync, file now matches peer");
                    }

                    MSG_TYPE_PATCH => {
                        let patch_string = match String::from_utf8(payload) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[{peer}] received invalid UTF-8 patch: {e:?}");
                                continue 'stream_loop;
                            }
                        };

                        println!("[{peer}] received patch -> {:?}", patch_string);

                        let patch = match Patch::from_str(&patch_string) {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!("[{peer}] failed to parse patch: {e:?}");
                                continue 'stream_loop;
                            }
                        };

                        // Apply against our tracked "last known good" content
                        // rather than re-reading disk, so we're always
                        // patching from the state the sender actually diffed
                        // against.
                        let target = {
                            let guard = last_known_content_recv.lock().unwrap();
                            guard.clone()
                        };

                        let patched = match apply(&target, &patch) {
                            Ok(p) => p,
                            Err(e) => {
                                eprintln!(
                                    "[{peer}] failed to apply patch (peer likely out of sync, \
                                     requesting full sync would help here): {e:?}"
                                );
                                continue 'stream_loop;
                            }
                        };

                        {
                            let mut guard = last_known_content_recv.lock().unwrap();
                            *guard = patched.clone();
                        }

                        if let Err(e) = fs::write(&file_path_target, &patched) {
                            eprintln!("[{peer}] failed to write target file {file_path_target}: {e:?}");
                            continue 'stream_loop;
                        }

                        println!("[{peer}] applied patch and updated {file_path_target}");
                    }

                    other => {
                        eprintln!("[{peer}] unknown message type {other}, dropping stream");
                        break 'stream_loop;
                    }
                }
            }

            println!("[{peer}] file-sync stream handler ending, waiting for next connection");
        }

        println!("file_sync_stream.next() returned None, receiver task exiting");
    });

    let (tx, rx) = channel();
    let (tx_patch, _) = broadcast::channel::<String>(16);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
        match res {
            Ok(event) => {
                if let Err(e) = tx.send(event) {
                    eprintln!("Failed to forward watcher event to channel: {e:?}");
                }
            }
            Err(e) => { println!("Watcher error: {e:?}"); }
        }
    })?;

    let file_path = args.file_path.clone();
    watcher.watch(Path::new(&file_path), RecursiveMode::Recursive)?;

    let tx_patch_file_watcher = tx_patch.clone();
    let file_path_clone = args.file_path.clone();
    let last_known_content_watch = last_known_content.clone();

    // --- Watcher thread: diffs local file changes and broadcasts patches ---
    //
    // NOTE: this runs on a plain OS thread, not tokio::spawn. `rx` is a
    // std::sync::mpsc::Receiver, so `for event in rx` blocks synchronously.
    // Running that inside a tokio task would tie up one of tokio's worker
    // threads for as long as this loop runs, starving other tasks scheduled
    // on it. broadcast::Sender::send() is a plain synchronous call, so
    // nothing here actually needs the async runtime.
    std::thread::spawn(move || {
        for event in rx {
            match event.kind {
                EventKind::Create(_) => {
                    println!("File created: {:?}", event.paths);
                }
                EventKind::Modify(_) => {
                    println!("File modified: {:?}", event.paths);

                    let new_content = match fs::read_to_string(&file_path_clone) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("Failed to read {file_path_clone} after modify event: {e:?}");
                            continue;
                        }
                    };

                    let mut guard = last_known_content_watch.lock().unwrap();

                    if *guard == new_content {
                        // This write matches content we already know about
                        // (it came from applying a remote patch/full sync).
                        // Nothing changed from our perspective; don't
                        // re-broadcast it.
                        println!("Skipping self-triggered file change (content already in sync)");
                        continue;
                    }

                    let patch = create_patch(&guard, &new_content);
                    let patch_str = patch.to_string();

                    *guard = new_content;
                    drop(guard);

                    if let Err(e) = tx_patch_file_watcher.send(patch_str) {
                        eprintln!("Failed to broadcast patch (no active receivers?): {e:?}");
                        // not fatal — just means no peers are currently connected
                    }
                }
                EventKind::Remove(_) => {
                    println!("File removed: {:?}", event.paths);
                }
                _ => {}
            }
        }

        println!("Watcher event channel closed, watcher thread exiting");
    });


    loop {
        tokio::select! {
            Ok(Some(line)) = stdin.next_line() => {
                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), line.as_bytes())
                {
                    println!("Publish error: {e:?}");
                }
            }
            _ = bootstrap_interval.tick() => {
                let _ = swarm.behaviour_mut().kademlia.bootstrap();
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {address}");
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                    for (peer_id, _addr) in peers {
                        println!("Discovered peer via mDNS: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                    for (peer_id, _addr) in peers {
                        println!("Peer expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    let mut control = control.clone();
                    let tx_patch_clone = tx_patch.clone();
                    let file_path_for_sync = args.file_path.clone();
                    let last_known_content_send = last_known_content.clone();

                    // --- Sender task: opens an outbound file-sync stream to
                    //     this peer, sends a full sync first so both sides
                    //     start from the same content, then forwards every
                    //     broadcast patch to it ---
                    tokio::spawn(async move {
                        let mut rx_patch = tx_patch_clone.subscribe();

                        let mut stream = match control.open_stream(peer_id, FILE_SYNC_PROTOCOL).await {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[{peer_id}] failed to open file-sync stream: {e:?}");
                                return;
                            }
                        };

                        // Send whatever we currently believe is in sync, so a
                        // newly-connected (or previously out-of-sync) peer
                        // reconciles to matching content before we start
                        // sending diffs.
                        let current_content = {
                            let guard = last_known_content_send.lock().unwrap();
                            guard.clone()
                        };

                        if let Err(e) = write_framed(&mut stream, MSG_TYPE_FULL, current_content.as_bytes()).await {
                            eprintln!("[{peer_id}] failed to send initial full sync: {e:?}");
                            return;
                        }
                        println!("[{peer_id}] sent initial full sync ({} bytes) for {file_path_for_sync}", current_content.len());

                        loop {
                            match rx_patch.recv().await {
                                Ok(patch_string) => {
                                    println!("[{peer_id}] sending patch -> {}", patch_string);

                                    if let Err(e) = write_framed(&mut stream, MSG_TYPE_PATCH, patch_string.as_bytes()).await {
                                        eprintln!("[{peer_id}] write error sending patch: {e:?}");
                                        break;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                    eprintln!("[{peer_id}] lagged behind, missed {n} patches, continuing");
                                    continue;
                                }
                                Err(broadcast::error::RecvError::Closed) => {
                                    println!("[{peer_id}] patch broadcast channel closed, ending sender task");
                                    break;
                                }
                            }
                        }
                    });
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received {
                    peer_id, info, ..
                })) => {
                    for addr in info.listen_addrs {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    }
                    println!("Discovered peer via identify: {peer_id}");
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::RoutingUpdated {
                    peer, ..
                })) => {
                    println!("Kademlia routing table updated with peer: {peer}");
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message,
                    ..
                })) => {
                    println!("{peer_id}: {}", String::from_utf8_lossy(&message.data));
                }
                _ => {}
            }
        }
    }
}