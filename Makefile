.PHONY: help start stop restart check download upload verify clean clean-local clean-hdfs clean-all logs jupyter token

# Couleurs pour l'affichage
BLUE=\033[0;34m
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m # No Color

help: ## Affiche cette aide
	@echo "$(BLUE)═══════════════════════════════════════════════$(NC)"
	@echo "$(BLUE)   TP SPARK - ANALYSE CLIMATIQUE AVEC HDFS$(NC)"
	@echo "$(BLUE)═══════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(GREEN)Commandes disponibles:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""

start: ## Démarre tous les conteneurs Docker
	@echo "$(BLUE)🚀 Démarrage des conteneurs...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✓ Conteneurs démarrés$(NC)"
	@echo "$(YELLOW)⏳ Attends 30 secondes pour que tout soit prêt...$(NC)"
	@sleep 30
	@$(MAKE) check

stop: ## Arrête tous les conteneurs
	@echo "$(BLUE)🛑 Arrêt des conteneurs...$(NC)"
	docker-compose stop
	@echo "$(GREEN)✓ Conteneurs arrêtés$(NC)"

restart: ## Redémarre tous les conteneurs
	@echo "$(BLUE)🔄 Redémarrage des conteneurs...$(NC)"
	docker-compose restart
	@sleep 20
	@$(MAKE) check

check: ## Vérifie l'état de l'environnement
	@echo "$(BLUE)🔍 Vérification de l'environnement...$(NC)"
	@chmod +x check_environment.sh
	@./check_environment.sh

download: ## Télécharge et upload les données dans HDFS (version bash)
	@echo "$(BLUE)📥 Téléchargement et upload des données GSOD...$(NC)"
	@chmod +x download_and_upload_to_hdfs.sh
	@./download_and_upload_to_hdfs.sh

upload-python: ## Télécharge et upload les données dans HDFS (version Python - recommandée)
	@echo "$(BLUE)📥 Téléchargement et upload des données GSOD (Python)...$(NC)"
	@chmod +x download_and_upload_to_hdfs.py
	@python3 download_and_upload_to_hdfs.py

verify: ## Vérifie les données dans HDFS
	@echo "$(BLUE)🔍 Vérification des données HDFS...$(NC)"
	@docker exec -i namenode hdfs dfs -ls /data/gsod
	@echo ""
	@echo "$(GREEN)Statistiques par année:$(NC)"
	@for year in 2019 2020 2021 2022 2023; do \
		count=$$(docker exec -i namenode hdfs dfs -ls /data/gsod/$$year 2>/dev/null | grep -c "\.csv" || echo "0"); \
		echo "  • $$year: $$count fichiers CSV"; \
	done
	@echo ""
	@echo "$(GREEN)Espace utilisé:$(NC)"
	@docker exec -i namenode hdfs dfs -du -s -h /data/gsod

clean-local: ## Supprime les fichiers téléchargés localement
	@echo "$(YELLOW)🧹 Nettoyage des fichiers locaux...$(NC)"
	rm -rf /tmp/gsod_data
	@echo "$(GREEN)✓ Fichiers locaux supprimés$(NC)"

clean-hdfs: ## Supprime les données de HDFS
	@echo "$(YELLOW)⚠️  Suppression des données HDFS...$(NC)"
	@read -p "Es-tu sûr(e) ? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker exec -i namenode hdfs dfs -rm -r /data/gsod; \
		echo "$(GREEN)✓ Données HDFS supprimées$(NC)"; \
	else \
		echo "$(YELLOW)✗ Annulé$(NC)"; \
	fi

clean-all: stop ## Arrête tout et supprime les volumes Docker (⚠️ DANGER)
	@echo "$(YELLOW)⚠️  ATTENTION: Cela va supprimer TOUTES les données HDFS$(NC)"
	@read -p "Es-tu vraiment sûr(e) ? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		rm -rf /tmp/gsod_data; \
		echo "$(GREEN)✓ Tout a été nettoyé$(NC)"; \
	else \
		echo "$(YELLOW)✗ Annulé$(NC)"; \
	fi

clean: clean-local ## Alias pour clean-local

logs: ## Affiche les logs de tous les conteneurs
	docker-compose logs -f

logs-namenode: ## Affiche les logs du namenode
	docker logs -f namenode

logs-spark: ## Affiche les logs de Spark
	docker logs -f spark-master

logs-jupyter: ## Affiche les logs de Jupyter
	docker logs -f pyspark_notebook

jupyter: token ## Affiche l'URL de Jupyter avec le token

token: ## Affiche le token Jupyter
	@echo "$(BLUE)🔑 Token Jupyter:$(NC)"
	@docker logs pyspark_notebook 2>&1 | grep "token=" | tail -1 | sed 's/.*http:\/\/127.0.0.1:8888\/lab?token=//'
	@echo ""
	@echo "$(GREEN)Accède à Jupyter:$(NC)"
	@echo "  http://localhost:8888"

ui: ## Affiche les URLs des interfaces web
	@echo "$(BLUE)🌐 Interfaces Web disponibles:$(NC)"
	@echo ""
	@echo "$(GREEN)Jupyter Notebook:$(NC)"
	@echo "  http://localhost:8888"
	@echo ""
	@echo "$(GREEN)HDFS NameNode UI:$(NC)"
	@echo "  http://localhost:9870"
	@echo ""
	@echo "$(GREEN)Spark Master UI:$(NC)"
	@echo "  http://localhost:8080"

setup: start download verify ## Installation complète (start + download + verify)
	@echo ""
	@echo "$(GREEN)═══════════════════════════════════════════════$(NC)"
	@echo "$(GREEN)   ✅ INSTALLATION TERMINÉE !$(NC)"
	@echo "$(GREEN)═══════════════════════════════════════════════$(NC)"
	@echo ""
	@$(MAKE) ui
	@echo ""
	@$(MAKE) token

status: check ## Alias pour check

# Commandes Docker utiles
ps: ## Liste les conteneurs en cours d'exécution
	@docker-compose ps

exec-namenode: ## Ouvre un shell dans le namenode
	@docker exec -it namenode bash

exec-jupyter: ## Ouvre un shell dans le conteneur Jupyter
	@docker exec -it pyspark_notebook bash

exec-spark: ## Ouvre un shell dans Spark Master
	@docker exec -it spark-master bash

# Aide par défaut
.DEFAULT_GOAL := help
