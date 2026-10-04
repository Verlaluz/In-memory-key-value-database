use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;


type DatabaseStorageEngine = Arc<RwLock<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
  
    let central_database_state: DatabaseStorageEngine = Arc::new(RwLock::new(HashMap::new()));

    
    let database_tcp_listener = TcpListener::bind("0.0.0.0:6379").await.expect("Failed to bind port");
    println!("🚀 Mini-Redis Database Engine running on port 6379");

    loop {
        let (mut incoming_client_socket, client_network_address) = database_tcp_listener.accept().await.unwrap();
        println!("Connection established from: {}", client_network_address);

       
        let client_database_reference = central_database_state.clone();

        tokio::spawn(async move {
            let (socket_read_half, mut socket_write_half) = incoming_client_socket.split();
            let mut network_buffered_reader = BufReader::new(socket_read_half);
            let mut incoming_command_text = String::new();

            loop {
                incoming_command_text.clear();
                
                
                let bytes_read = network_buffered_reader.read_line(&mut incoming_command_text).await.unwrap_or(0);
                if bytes_read == 0 {
                    println!("Client {} disconnected.", client_network_address);
                    break;
                }


                let cleaned_command_text = incoming_command_text.trim();
                let mut string_parts_iterator = cleaned_command_text.split_whitespace();

                let primary_command_action = string_parts_iterator.next().unwrap_or("");
                let requested_database_key = string_parts_iterator.next().unwrap_or("");

                match primary_command_action.to_uppercase().as_str() {
                    "SET" => {
                        
                        let value_to_store: Vec<&str> = string_parts_iterator.collect();
                        let final_value_string = value_to_store.join(" ");

                        if requested_database_key.is_empty() || final_value_string.is_empty() {
                            let _ = socket_write_half.write_all(b"ERROR: Usage: SET <key> <value>\n").await;
                        } else {
                          
                            let mut active_write_lock = client_database_reference.write().await;
                            active_write_lock.insert(requested_database_key.to_string(), final_value_string);
                            
                            let _ = socket_write_half.write_all(b"SUCCESS: OK\n").await;
                        }
                    }
                    "GET" => {
                        if requested_database_key.is_empty() {
                            let _ = socket_write_half.write_all(b"ERROR: Usage: GET <key>\n").await;
                        } else {
                           
                            let active_read_lock = client_database_reference.read().await;
                            
                            match active_read_lock.get(requested_database_key) {
                                Some(retrieved_value) => {
                                    let response_payload = format!("VALUE: {}\n", retrieved_value);
                                    let _ = socket_write_half.write_all(response_payload.as_bytes()).await;
                                }
                                None => {
                                    let _ = socket_write_half.write_all(b"ERROR: Key not found (NULL)\n").await;
                                }
                            }
                        }
                    }
                    "DELETE" => {
                        let mut active_write_lock = client_database_reference.write().await;
                        if active_write_lock.remove(requested_database_key).is_some() {
                            let _ = socket_write_half.write_all(b"SUCCESS: Deleted\n").await;
                        } else {
                            let _ = socket_write_half.write_all(b"ERROR: Key not found\n").await;
                        }
                    }
                    _ => {
                        let _ = socket_write_half.write_all(b"ERROR: Unknown command. Try SET, GET, or DELETE\n").await;
                    }
                }
            }
        });
    }
}