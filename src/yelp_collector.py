#!/usr/bin/env python3
"""
🍴 COLLECTEUR DE DONNÉES YELP - VERSION DÉBUTANT
===============================================

Ce script fait exactement ce qui est demandé :
1. Se connecte à l'API Yelp Fusion
2. Récupère des restaurants à Paris
3. Récupère les avis de chaque restaurant
4. Sauvegarde en CSV et JSON
5. Upload vers AWS S3

UTILISATION :
1. Copiez config/config_template.txt vers config/.env
2. Remplissez votre clé API Yelp et credentials AWS
3. Lancez : python src/yelp_collector.py

Auteur : Projet Data Engineering Yelp
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
        logging.FileHandler('logs/yelp_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YelpCollector:
    """
    Classe simple pour collecter les données Yelp
    
    Cette classe fait tout le travail :
    - Se connecte à l'API Yelp
    - Récupère les restaurants
    - Récupère les avis
    - Sauvegarde les données
    - Upload vers S3
    """
    
    def __init__(self):
        """Initialise le collecteur avec la configuration"""
        logger.info("🚀 Initialisation du collecteur Yelp...")
        
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
        self.yelp_api_key = os.getenv("YELP_API_KEY", "")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.s3_bucket = os.getenv("S3_BUCKET_NAME", "")
        self.target_city = os.getenv("TARGET_CITY", "Paris")
        self.target_country = os.getenv("TARGET_COUNTRY", "France")
        self.max_restaurants = int(os.getenv("MAX_RESTAURANTS", "20"))
        
        # Configuration de l'API Yelp
        self.yelp_base_url = "https://api.yelp.com/v3"
        self.yelp_headers = {
            "Authorization": f"Bearer {self.yelp_api_key}",
            "Content-Type": "application/json"
        }
    
    def validate_config(self):
        """Vérifie que la configuration est complète"""
        logger.info("🔍 Validation de la configuration...")
        
        # Vérifier la clé API Yelp (obligatoire)
        if not self.yelp_api_key:
            logger.error("❌ ERREUR : Clé API Yelp manquante !")
            logger.error("💡 Ajoutez YELP_API_KEY dans config/.env")
            raise ValueError("Clé API Yelp requise")
        
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
        ÉTAPE 1 : Recherche des restaurants à Paris
        
        Returns:
            list: Liste des restaurants trouvés
        """
        logger.info(f"🔍 ÉTAPE 1 : Recherche de {self.max_restaurants} restaurants à {self.target_city}...")
        
        # URL et paramètres pour l'API Yelp
        url = f"{self.yelp_base_url}/businesses/search"
        params = {
            "location": f"{self.target_city}, {self.target_country}",
            "term": "restaurants",
            "limit": min(self.max_restaurants, 50),  # Yelp limite à 50 par requête
            "sort_by": "rating"  # Trier par note pour avoir les meilleurs
        }
        
        try:
            # Faire la requête à l'API Yelp
            logger.info("📡 Requête à l'API Yelp en cours...")
            response = requests.get(url, headers=self.yelp_headers, params=params)
            
            # Vérifier si la requête a réussi
            if response.status_code == 200:
                data = response.json()
                restaurants = data.get("businesses", [])
                logger.info(f"✅ {len(restaurants)} restaurants trouvés !")
                
                # Afficher quelques exemples pour vérifier
                for i, restaurant in enumerate(restaurants[:3]):
                    logger.info(f"   {i+1}. {restaurant['name']} - {restaurant['rating']}⭐ ({restaurant['review_count']} avis)")
                
                return restaurants
            
            else:
                logger.error(f"❌ Erreur API Yelp : {response.status_code}")
                logger.error(f"   Message : {response.text}")
                return []
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la recherche : {str(e)}")
            return []
    
    def get_restaurant_reviews(self, restaurant_id, restaurant_name):
        """
        Récupère les avis d'un restaurant spécifique
        
        Args:
            restaurant_id (str): ID Yelp du restaurant
            restaurant_name (str): Nom du restaurant (pour les logs)
            
        Returns:
            list: Liste des avis du restaurant
        """
        logger.info(f"💬 Récupération des avis pour : {restaurant_name}")
        
        # URL pour les avis du restaurant
        url = f"{self.yelp_base_url}/businesses/{restaurant_id}/reviews"
        params = {"locale": "fr_FR"}  # Avis en français si disponible
        
        try:
            # Faire la requête
            response = requests.get(url, headers=self.yelp_headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                reviews = data.get("reviews", [])
                
                # Ajouter l'ID du restaurant à chaque avis
                for review in reviews:
                    review["business_id"] = restaurant_id
                    review["business_name"] = restaurant_name
                
                logger.info(f"   ✅ {len(reviews)} avis récupérés")
                return reviews
            
            else:
                logger.warning(f"   ⚠️ Erreur {response.status_code} pour {restaurant_name}")
                return []
        
        except Exception as e:
            logger.error(f"   ❌ Erreur : {str(e)}")
            return []
    
    def collect_all_reviews(self, restaurants):
        """
        ÉTAPE 2 : Collecte tous les avis pour tous les restaurants
        
        Args:
            restaurants (list): Liste des restaurants
            
        Returns:
            list: Liste de tous les avis collectés
        """
        logger.info("💬 ÉTAPE 2 : Collecte des avis pour tous les restaurants...")
        
        all_reviews = []
        
        # Utiliser tqdm pour afficher une barre de progression
        for restaurant in tqdm(restaurants, desc="Collecte des avis"):
            restaurant_id = restaurant["id"]
            restaurant_name = restaurant["name"]
            
            # Récupérer les avis de ce restaurant
            reviews = self.get_restaurant_reviews(restaurant_id, restaurant_name)
            all_reviews.extend(reviews)
            
            # Petite pause pour ne pas surcharger l'API Yelp
            time.sleep(0.1)
        
        logger.info(f"✅ Total : {len(all_reviews)} avis collectés pour {len(restaurants)} restaurants")
        return all_reviews
    
    def prepare_data_for_export(self, restaurants, reviews):
        """
        ÉTAPE 3 : Prépare les données pour l'export (nettoyage et structuration)
        
        Args:
            restaurants (list): Liste des restaurants
            reviews (list): Liste des avis
            
        Returns:
            tuple: (restaurants_df, reviews_df) DataFrames pandas
        """
        logger.info("🧹 ÉTAPE 3 : Préparation des données pour l'export...")
        
        # === PRÉPARATION DES DONNÉES RESTAURANTS ===
        restaurants_clean = []
        
        for restaurant in restaurants:
            # Extraire les informations importantes de façon sécurisée
            clean_restaurant = {
                "id": restaurant.get("id", ""),
                "name": restaurant.get("name", ""),
                "rating": restaurant.get("rating", 0),
                "review_count": restaurant.get("review_count", 0),
                "price": restaurant.get("price", ""),
                "phone": restaurant.get("phone", ""),
                "is_closed": restaurant.get("is_closed", False),
                
                # Catégories (joindre en texte)
                "categories": ", ".join([cat.get("title", "") for cat in restaurant.get("categories", [])]),
                "primary_category": restaurant.get("categories", [{}])[0].get("title", "") if restaurant.get("categories") else "",
                
                # Localisation
                "address": ", ".join(restaurant.get("location", {}).get("display_address", [])),
                "city": restaurant.get("location", {}).get("city", ""),
                "state": restaurant.get("location", {}).get("state", ""),
                "zip_code": restaurant.get("location", {}).get("zip_code", ""),
                "country": restaurant.get("location", {}).get("country", ""),
                
                # Coordonnées
                "latitude": restaurant.get("coordinates", {}).get("latitude", ""),
                "longitude": restaurant.get("coordinates", {}).get("longitude", ""),
                
                # Métadonnées
                "collected_at": datetime.now().isoformat()
            }
            restaurants_clean.append(clean_restaurant)
        
        # === PRÉPARATION DES DONNÉES AVIS ===
        reviews_clean = []
        
        for review in reviews:
            clean_review = {
                "review_id": review.get("id", ""),
                "business_id": review.get("business_id", ""),
                "business_name": review.get("business_name", ""),
                
                # Contenu de l'avis
                "text": review.get("text", ""),
                "rating": review.get("rating", 0),
                "time_created": review.get("time_created", ""),
                "url": review.get("url", ""),
                
                # Informations utilisateur
                "user_id": review.get("user", {}).get("id", ""),
                "user_name": review.get("user", {}).get("name", ""),
                "user_review_count": review.get("user", {}).get("review_count", 0),
                
                # Métadonnées
                "collected_at": datetime.now().isoformat()
            }
            reviews_clean.append(clean_review)
        
        # Convertir en DataFrames pandas pour faciliter l'export
        restaurants_df = pd.DataFrame(restaurants_clean)
        reviews_df = pd.DataFrame(reviews_clean)
        
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
            restaurants_csv = f"data/restaurants_{timestamp}.csv"
            reviews_csv = f"data/reviews_{timestamp}.csv"
            
            restaurants_df.to_csv(restaurants_csv, index=False, encoding='utf-8')
            reviews_df.to_csv(reviews_csv, index=False, encoding='utf-8')
            
            logger.info(f"✅ CSV sauvegardés :")
            logger.info(f"   - Restaurants : {restaurants_csv}")
            logger.info(f"   - Avis : {reviews_csv}")
            
            # === SAUVEGARDE JSON ===
            json_file = f"data/yelp_data_complete_{timestamp}.json"
            
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
                    # Nom du fichier dans S3 (garder juste le nom, pas le chemin)
                    s3_key = f"yelp-data/{os.path.basename(file_path)}"
                    
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
        logger.info("🚀 DÉBUT DE LA COLLECTE DE DONNÉES YELP")
        logger.info("=" * 60)
        
        try:
            # ÉTAPE 1 : Rechercher les restaurants
            restaurants = self.search_restaurants()
            
            if not restaurants:
                logger.error("❌ Aucun restaurant trouvé - arrêt du processus")
                return
            
            # ÉTAPE 2 : Collecter tous les avis
            reviews = self.collect_all_reviews(restaurants)
            
            # ÉTAPE 3 : Préparer les données
            restaurants_df, reviews_df = self.prepare_data_for_export(restaurants, reviews)
            
            # Créer le dataset complet
            complete_data = {
                "metadata": {
                    "city": self.target_city,
                    "country": self.target_country,
                    "collection_date": datetime.now().isoformat(),
                    "total_restaurants": len(restaurants),
                    "total_reviews": len(reviews)
                },
                "restaurants": restaurants,
                "reviews": reviews
            }
            
            # ÉTAPE 4 : Sauvegarder localement
            file_paths = self.save_data_locally(restaurants_df, reviews_df, complete_data)
            
            # ÉTAPE 5 : Upload vers S3
            if file_paths:
                self.upload_to_s3(file_paths)
            
            # RAPPORT FINAL
            logger.info("📊 RAPPORT FINAL")
            logger.info("=" * 40)
            logger.info(f"🏪 Restaurants collectés : {len(restaurants)}")
            logger.info(f"💬 Avis collectés : {len(reviews)}")
            logger.info(f"📁 Fichiers sauvegardés : {len(file_paths) if file_paths else 0}")
            logger.info("🎉 COLLECTE TERMINÉE AVEC SUCCÈS !")
        
        except KeyboardInterrupt:
            logger.info("⏹️ Collecte interrompue par l'utilisateur")
        except Exception as e:
            logger.error(f"💥 Erreur fatale : {str(e)}")


def main():
    """Point d'entrée principal du script"""
    print("🍴 Collecteur de Données Yelp - Version Débutant")
    print("=" * 50)
    
    # Créer et lancer le collecteur
    collector = YelpCollector()
    collector.run()


# Lancer le script si exécuté directement
if __name__ == "__main__":
    main() 