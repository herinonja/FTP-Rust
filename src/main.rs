use axum::{routing::post, Json, Extension, Router};
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

// 1. Définition de la table de routage en mémoire
type PhoneRegistry = Arc<RwLock<HashMap<String, String>>>; // Clé: ID du téléphone, Valeur: IP brute

// 2. La structure de données que votre app Flutter va envoyer en JSON
#[derive(Deserialize, Debug)]
struct RegisterPayload {
    phone_id: String,
    phone_ip: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialisation des logs
    tracing_subscriber::fmt::init();

    // Initialisation de notre table de routage vide
    let registry: PhoneRegistry = Arc::new(RwLock::new(HashMap::new()));

    // Configuration des routes HTTP de notre API
    let app = Router::new()
        .route("/register", post(register_phone))
        // On injecte notre registre dans l'application pour qu'il soit accessible dans les fonctions
        .layer(Extension(registry));

    // Lancement du serveur HTTP sur le port 3000
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Serveur d'enregistrement HTTP actif sur http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// 3. Le gestionnaire de la requête HTTP POST /register
async fn register_phone(
    Extension(registry): Extension<PhoneRegistry>,
    Json(payload): Json<RegisterPayload>,
) -> &'static str {
    // On verrouille la table en écriture pour ajouter ou mettre à jour le téléphone
    let mut table = registry.write().await;
    table.insert(payload.phone_id.clone(), payload.phone_ip.clone());
    
    info!("Téléphone enregistré : {} -> IP: {}", payload.phone_id, payload.phone_ip);
    
    "Enregistrement réussi !"
}
