#!/usr/bin/env python3
"""
🗺️ COLLECTEUR DE DONNÉES GOOGLE PLACES - VERSION DÉBUTANT
=========================================================

Ce script fait exactement ce qui est demandé :
1. Se connecte à l'API Google Places
2. Récupère des restaurants à Paris
3. Récupère les avis de chaque restaurant
4. Sauvegarde en CSV et JSON
5. Upload vers AWS S3 (optionnel)

AVANTAGES vs Yelp :
- Plus facile à configurer (compte Google suffit)
- 300$ gratuit par mois (largement suffisant)
- Plus de données disponibles
- Meilleure couverture mondiale

UTILISATION :
1. Copiez config/config_template.txt vers config/.env
2. Ajoutez votre clé API Google Places
3. Lancez : python src/google_places_collector.py

Auteur : Projet Data Engineering
Date : 2024
"""

# ===== IMPORTS (bibliothèques nécessaires) =====
import os
import json
import pandas as pd
import requests
import logging
import boto3
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import time

# ===== CONFIGURATION DU LOGGING (pour voir ce qui se passe) =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/google_places_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GooglePlacesCollector:
    """
    Classe simple pour collecter les données Google Places
    
    Cette classe fait tout le travail :
    - Se connecte à l'API Google Places
    - Récupère les restaurants
    - Récupère les avis
    - Sauvegarde les données
    - Upload vers S3
    """
    
    def __init__(self):
        """Initialise le collecteur avec la configuration"""
        logger.info("🚀 Initialisation du collecteur Google Places...")
        
        # Charger la configuration depuis le fichier .env
        self.load_config()
        
        # Vérifier que tout est configuré
        self.validate_config()
        
        # Préparer les dossiers de sauvegarde
        self.setup_directories()
        
        logger.info("✅ Collecteur initialisé avec succès !")
    
    def load_config(self):
        """Charge la configuration depuis le fichier .env"""
        logger.info("📋 Chargement de la configuration...")
        
        # Charger le fichier .env s'il existe
        env_file = "config/.env"
        if os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"✅ Configuration chargée depuis {env_file}")
        else:
            logger.warning("⚠️ Fichier .env non trouvé, utilisation des variables d'environnement")
        
        # Récupérer les variables de configuration
        self.google_api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.s3_bucket = os.getenv("S3_BUCKET_NAME", "")
        self.target_city = os.getenv("TARGET_CITY", "Paris")
        self.target_country = os.getenv("TARGET_COUNTRY", "France")
        self.max_restaurants = int(os.getenv("MAX_RESTAURANTS", "60"))
        
        # Requêtes diversifiées pour un dashboard de qualité
        self.diverse_queries = [
            "fine dining restaurants",      # Restaurants haut de gamme (4.5-5.0)
            "restaurants",                  # Restaurants classiques (4.0-4.5)
            "fast food",                   # Fast food (3.0-4.0)
            "cheap eats budget restaurants", # Restaurants pas chers (2.5-4.0)
            "street food food trucks",      # Street food (2.0-4.0)
            "bars pubs bistros"            # Bars/bistros (3.0-4.5)
        ]
        
        # Configuration de l'API Google Places
        self.places_base_url = "https://maps.googleapis.com/maps/api/place"
    
    def validate_config(self):
        """Vérifie que la configuration est complète"""
        logger.info("🔍 Validation de la configuration...")
        
        # Vérifier la clé API Google Places (obligatoire)
        if not self.google_api_key:
            logger.error("❌ ERREUR : Clé API Google Places manquante !")
            logger.error("💡 Ajoutez GOOGLE_PLACES_API_KEY dans config/.env")
            logger.error("🔗 Obtenez votre clé sur : https://console.cloud.google.com/")
            raise ValueError("Clé API Google Places requise")
        
        # AWS est optionnel pour commencer
        if not self.s3_bucket:
            logger.warning("⚠️ Bucket S3 non configuré - pas d'upload AWS")
        
        logger.info("✅ Configuration validée")
    
    def setup_directories(self):
        """Crée les dossiers nécessaires pour sauvegarder les données"""
        directories = ["data", "logs"]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"📁 Dossier créé : {directory}")
    
    def search_restaurants(self):
        """
        ÉTAPE 1 : Recherche des restaurants diversifiés avec Google Places
        
        Returns:
            list: Liste des restaurants trouvés
        """
        logger.info(f"🔍 ÉTAPE 1 : Recherche diversifiée de {self.max_restaurants} restaurants à {self.target_city}...")
        
        # URL pour la recherche de lieux
        url = f"{self.places_base_url}/textsearch/json"
        
        # Différents types de recherches pour plus de diversité
        search_queries = [
            f"restaurants {self.target_city}",  # Restaurants classiques
            f"fast food {self.target_city}",   # Fast food (notes plus basses)
            f"cafes {self.target_city}",       # Cafés  
            f"bars {self.target_city}",        # Bars
            f"cheap restaurants {self.target_city}",  # Restaurants pas chers
            f"food trucks {self.target_city}",        # Food trucks
        ]
        
        all_restaurants = []
        
        try:
            # Faire plusieurs requêtes pour diversifier les résultats
            restaurants_per_query = self.max_restaurants // len(search_queries)
            
            for i, query in enumerate(search_queries):
                if len(all_restaurants) >= self.max_restaurants:
                    break
                    
                logger.info(f"📡 Requête {i+1}/{len(search_queries)}: {query}")
                
                params = {
                    "query": query,
                    "type": "restaurant", 
                    "key": self.google_api_key,
                    "language": "fr"
                }
                
                response = requests.get(url, params=params)
                
                                if response.status_code == 200:
                    data = response.json()
                    
                    if data["status"] == "OK":
                        restaurants = data.get("results", [])
                        # Éviter les doublons
                        existing_ids = {r.get("place_id") for r in all_restaurants}
                        new_restaurants = [r for r in restaurants if r.get("place_id") not in existing_ids]
                        all_restaurants.extend(new_restaurants)
                        
                        logger.info(f"✅ {len(new_restaurants)} nouveaux restaurants trouvés !")
                        
                        # Afficher quelques exemples pour vérifier
                        for i, restaurant in enumerate(new_restaurants[:2]):
                            name = restaurant.get("name", "Nom inconnu")
                            rating = restaurant.get("rating", 0)
                            user_ratings_total = restaurant.get("user_ratings_total", 0)
                            logger.info(f"   {i+1}. {name} - {rating}⭐ ({user_ratings_total} avis)")
                        
                        # Pause entre les requêtes
                        if i < len(search_queries) - 1:
                            time.sleep(1)
                    
                    while (next_page_token and 
                           len(all_restaurants) < self.max_restaurants and 
                           page_count < 6):  # Limite à 6 pages max (60 restaurants)
                        
                        logger.info(f"📄 Récupération de la page {page_count + 1}...")
                        time.sleep(2)  # Google exige une pause entre les requêtes de pagination
                        
                        next_params = {
                            "pagetoken": next_page_token,
                            "key": self.google_api_key
                        }
                        
                        next_response = requests.get(url, params=next_params)
                        
                        if next_response.status_code == 200:
                            next_data = next_response.json()
                            if next_data["status"] == "OK":
                                next_restaurants = next_data.get("results", [])
                                all_restaurants.extend(next_restaurants)
                                next_page_token = next_data.get("next_page_token")
                                page_count += 1
                                logger.info(f"   ✅ +{len(next_restaurants)} restaurants supplémentaires")
                            else:
                                break
                        else:
                            break
                    
                    # Limiter au nombre demandé
                    all_restaurants = all_restaurants[:self.max_restaurants]
                    logger.info(f"✅ Total final : {len(all_restaurants)} restaurants")
                    
                    return all_restaurants
                
                else:
                    logger.error(f"❌ Erreur API Google Places : {data['status']}")
                    if "error_message" in data:
                        logger.error(f"   Message : {data['error_message']}")
                    return []
            
            else:
                logger.error(f"❌ Erreur HTTP : {response.status_code}")
                logger.error(f"   Message : {response.text}")
                return []
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la recherche : {str(e)}")
            return []
    
    def get_place_details(self, place_id, place_name):
        """
        Récupère les détails complets d'un restaurant (y compris les avis)
        
        Args:
            place_id (str): ID Google Places du restaurant
            place_name (str): Nom du restaurant (pour les logs)
            
        Returns:
            dict: Détails complets du restaurant avec avis
        """
        logger.info(f"📋 Récupération des détails pour : {place_name}")
        
        # URL pour les détails d'un lieu
        url = f"{self.places_base_url}/details/json"
        
        # Paramètres pour obtenir les détails + avis
        params = {
            "place_id": place_id,
            "fields": "place_id,name,rating,user_ratings_total,reviews,formatted_address,geometry,types,price_level,opening_hours,formatted_phone_number,website,photos",
            "key": self.google_api_key,
            "language": "fr"  # Avis en français si disponible
        }
        
        try:
            # Faire la requête
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                if data["status"] == "OK":
                    result = data.get("result", {})
                    reviews = result.get("reviews", [])
                    
                    logger.info(f"   ✅ Détails récupérés avec {len(reviews)} avis")
                    return result
                
                else:
                    logger.warning(f"   ⚠️ Erreur API : {data['status']} pour {place_name}")
                    return {}
            
            else:
                logger.warning(f"   ⚠️ Erreur HTTP {response.status_code} pour {place_name}")
                return {}
        
        except Exception as e:
            logger.error(f"   ❌ Erreur : {str(e)}")
            return {}
    
    def collect_all_details(self, restaurants):
        """
        ÉTAPE 2 : Collecte les détails complets pour tous les restaurants
        
        Args:
            restaurants (list): Liste des restaurants de base
            
        Returns:
            list: Liste des restaurants avec détails complets et avis
        """
        logger.info("📋 ÉTAPE 2 : Collecte des détails pour tous les restaurants...")
        
        detailed_restaurants = []
        
        # Utiliser tqdm pour afficher une barre de progression
        for restaurant in tqdm(restaurants, desc="Collecte des détails"):
            place_id = restaurant.get("place_id")
            place_name = restaurant.get("name", "Restaurant inconnu")
            
            if place_id:
                # Récupérer les détails complets
                details = self.get_place_details(place_id, place_name)
                
                if details:
                    # Fusionner les données de base avec les détails
                    restaurant_complete = {**restaurant, **details}
                    detailed_restaurants.append(restaurant_complete)
                else:
                    # Garder au moins les données de base si les détails échouent
                    detailed_restaurants.append(restaurant)
                
                # Petite pause pour respecter les limites de l'API
                time.sleep(0.1)
        
        logger.info(f"✅ {len(detailed_restaurants)} restaurants avec détails collectés")
        return detailed_restaurants
    
    def prepare_data_for_export(self, restaurants):
        """
        ÉTAPE 3 : Prépare les données pour l'export (nettoyage et structuration)
        
        Args:
            restaurants (list): Liste des restaurants complets
            
        Returns:
            tuple: (restaurants_df, reviews_df) DataFrames pandas
        """
        logger.info("🧹 ÉTAPE 3 : Préparation des données pour l'export...")
        
        # === PRÉPARATION DES DONNÉES RESTAURANTS ===
        restaurants_clean = []
        all_reviews = []
        
        for restaurant in restaurants:
            # Extraire les informations restaurants de façon sécurisée
            clean_restaurant = {
                "place_id": restaurant.get("place_id", ""),
                "name": restaurant.get("name", ""),
                "rating": restaurant.get("rating", 0),
                "user_ratings_total": restaurant.get("user_ratings_total", 0),
                "price_level": restaurant.get("price_level", ""),
                "formatted_address": restaurant.get("formatted_address", ""),
                "formatted_phone_number": restaurant.get("formatted_phone_number", ""),
                "website": restaurant.get("website", ""),
                
                # Types/catégories
                "types": ", ".join(restaurant.get("types", [])),
                "primary_type": restaurant.get("types", [""])[0] if restaurant.get("types") else "",
                
                # Géolocalisation
                "latitude": restaurant.get("geometry", {}).get("location", {}).get("lat", ""),
                "longitude": restaurant.get("geometry", {}).get("location", {}).get("lng", ""),
                
                # Horaires
                "opening_hours": str(restaurant.get("opening_hours", {}).get("weekday_text", [])),
                
                # Métadonnées
                "collected_at": datetime.now().isoformat(),
                "data_source": "google_places"
            }
            restaurants_clean.append(clean_restaurant)
            
            # === EXTRAIRE LES AVIS ===
            reviews = restaurant.get("reviews", [])
            for review in reviews:
                clean_review = {
                    "place_id": restaurant.get("place_id", ""),
                    "restaurant_name": restaurant.get("name", ""),
                    
                    # Contenu de l'avis
                    "author_name": review.get("author_name", ""),
                    "author_url": review.get("author_url", ""),
                    "language": review.get("language", ""),
                    "profile_photo_url": review.get("profile_photo_url", ""),
                    "rating": review.get("rating", 0),
                    "relative_time_description": review.get("relative_time_description", ""),
                    "text": review.get("text", ""),
                    "time": review.get("time", 0),
                    
                    # Métadonnées
                    "collected_at": datetime.now().isoformat(),
                    "data_source": "google_places"
                }
                all_reviews.append(clean_review)
        
        # Convertir en DataFrames pandas pour faciliter l'export
        restaurants_df = pd.DataFrame(restaurants_clean)
        reviews_df = pd.DataFrame(all_reviews)
        
        logger.info(f"✅ Données préparées : {len(restaurants_df)} restaurants, {len(reviews_df)} avis")
        
        return restaurants_df, reviews_df
    
    def save_data_locally(self, restaurants_df, reviews_df, all_data):
        """
        ÉTAPE 4 : Sauvegarde les données en local (CSV et JSON)
        
        Args:
            restaurants_df: DataFrame des restaurants
            reviews_df: DataFrame des avis
            all_data: Données complètes en format dictionnaire
        """
        logger.info("💾 ÉTAPE 4 : Sauvegarde des données en local...")
        
        # Créer un timestamp pour les noms de fichiers
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # === SAUVEGARDE CSV ===
            restaurants_csv = f"data/restaurants_google_{timestamp}.csv"
            reviews_csv = f"data/reviews_google_{timestamp}.csv"
            
            restaurants_df.to_csv(restaurants_csv, index=False, encoding='utf-8')
            reviews_df.to_csv(reviews_csv, index=False, encoding='utf-8')
            
            logger.info(f"✅ CSV sauvegardés :")
            logger.info(f"   - Restaurants : {restaurants_csv}")
            logger.info(f"   - Avis : {reviews_csv}")
            
            # === SAUVEGARDE JSON ===
            json_file = f"data/google_places_data_complete_{timestamp}.json"
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ JSON sauvegardé : {json_file}")
            
            # Retourner les noms de fichiers pour l'upload S3
            return {
                "restaurants_csv": restaurants_csv,
                "reviews_csv": reviews_csv,
                "json_file": json_file
            }
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la sauvegarde : {str(e)}")
            return None
    
    def upload_to_s3(self, file_paths):
        """
        ÉTAPE 5 : Upload les fichiers vers AWS S3
        
        Args:
            file_paths (dict): Chemins des fichiers à uploader
        """
        if not self.s3_bucket:
            logger.warning("⏩ ÉTAPE 5 : Upload S3 ignoré (bucket non configuré)")
            return
        
        logger.info("☁️ ÉTAPE 5 : Upload vers AWS S3...")
        
        try:
            # Créer le client S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )
            
            # Vérifier/créer le bucket
            try:
                s3_client.head_bucket(Bucket=self.s3_bucket)
                logger.info(f"✅ Bucket {self.s3_bucket} trouvé")
            except:
                logger.info(f"📦 Création du bucket {self.s3_bucket}...")
                s3_client.create_bucket(Bucket=self.s3_bucket)
            
            # Uploader chaque fichier
            for file_type, file_path in file_paths.items():
                if file_path and os.path.exists(file_path):
                    # Nom du fichier dans S3
                    s3_key = f"google-places-data/{os.path.basename(file_path)}"
                    
                    logger.info(f"📤 Upload de {file_path}...")
                    s3_client.upload_file(file_path, self.s3_bucket, s3_key)
                    logger.info(f"   ✅ Uploadé vers s3://{self.s3_bucket}/{s3_key}")
            
            logger.info("🎉 Tous les fichiers uploadés vers S3 !")
        
        except Exception as e:
            logger.error(f"❌ Erreur upload S3 : {str(e)}")
            logger.warning("💡 Vérifiez vos credentials AWS et le nom du bucket")
    
    def run(self):
        """
        MÉTHODE PRINCIPALE : Lance tout le processus de collecte
        """
        logger.info("🚀 DÉBUT DE LA COLLECTE DE DONNÉES GOOGLE PLACES")
        logger.info("=" * 60)
        
        try:
            # ÉTAPE 1 : Rechercher les restaurants
            restaurants = self.search_restaurants()
            
            if not restaurants:
                logger.error("❌ Aucun restaurant trouvé - arrêt du processus")
                return
            
            # ÉTAPE 2 : Collecter les détails complets
            detailed_restaurants = self.collect_all_details(restaurants)
            
            # ÉTAPE 3 : Préparer les données
            restaurants_df, reviews_df = self.prepare_data_for_export(detailed_restaurants)
            
            # Créer le dataset complet
            complete_data = {
                "metadata": {
                    "city": self.target_city,
                    "country": self.target_country,
                    "collection_date": datetime.now().isoformat(),
                    "total_restaurants": len(detailed_restaurants),
                    "total_reviews": len(reviews_df),
                    "api_source": "google_places"
                },
                "restaurants": detailed_restaurants
            }
            
            # ÉTAPE 4 : Sauvegarder localement
            file_paths = self.save_data_locally(restaurants_df, reviews_df, complete_data)
            
            # ÉTAPE 5 : Upload vers S3
            if file_paths:
                self.upload_to_s3(file_paths)
            
            # RAPPORT FINAL
            logger.info("📊 RAPPORT FINAL")
            logger.info("=" * 40)
            logger.info(f"🏪 Restaurants collectés : {len(detailed_restaurants)}")
            logger.info(f"💬 Avis collectés : {len(reviews_df)}")
            logger.info(f"📁 Fichiers sauvegardés : {len(file_paths) if file_paths else 0}")
            logger.info("🎉 COLLECTE TERMINÉE AVEC SUCCÈS !")
        
        except KeyboardInterrupt:
            logger.info("⏹️ Collecte interrompue par l'utilisateur")
        except Exception as e:
            logger.error(f"💥 Erreur fatale : {str(e)}")


def main():
    """Point d'entrée principal du script"""
    print("🗺️ Collecteur de Données Google Places - Version Débutant")
    print("=" * 60)
    
    # Créer et lancer le collecteur
    collector = GooglePlacesCollector()
    collector.run()


# Lancer le script si exécuté directement
if __name__ == "__main__":
    main() 