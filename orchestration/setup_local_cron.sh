#!/bin/bash

# Script pour configurer l'orchestration locale avec cron
# Exécute le pipeline automatiquement tous les jours

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ORCHESTRATOR_SCRIPT="$PROJECT_DIR/orchestration/local_orchestrator.py"

echo "🤖 Configuration de l'orchestration locale"
echo "==========================================="
echo "Répertoire du projet: $PROJECT_DIR"

# Vérifier que Python et dbt sont installés
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 non trouvé. Installez Python 3.7+"
    exit 1
fi

if ! command -v dbt &> /dev/null; then
    echo "❌ dbt non trouvé. Installez avec: pip install dbt-core dbt-duckdb"
    exit 1
fi

echo "✅ Python3 et dbt trouvés"

# Créer une tâche cron pour exécuter le pipeline quotidiennement à 8h
CRON_JOB="0 8 * * * cd $PROJECT_DIR && python3 orchestration/local_orchestrator.py >> logs/cron_orchestrator.log 2>&1"

# Vérifier si la tâche cron existe déjà
if crontab -l 2>/dev/null | grep -q "local_orchestrator.py"; then
    echo "⚠️ Tâche cron déjà configurée"
    echo "Tâches cron actuelles:"
    crontab -l | grep "local_orchestrator.py"
else
    # Ajouter la nouvelle tâche cron
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "✅ Tâche cron ajoutée:"
    echo "   Exécution quotidienne à 8h00"
    echo "   Logs dans: logs/cron_orchestrator.log"
fi

# Créer le dossier logs s'il n'existe pas
mkdir -p "$PROJECT_DIR/logs"

echo ""
echo "🎯 Configuration terminée !"
echo ""
echo "📅 Pipeline programmé pour s'exécuter tous les jours à 8h"
echo "📝 Logs disponibles dans: logs/"
echo ""
echo "🚀 Pour tester maintenant:"
echo "   python3 orchestration/local_orchestrator.py"
echo ""
echo "🔧 Pour voir les tâches cron:"
echo "   crontab -l"
echo ""
echo "🗑️ Pour supprimer la tâche cron:"
echo "   crontab -e  # puis supprimez la ligne avec local_orchestrator.py" 