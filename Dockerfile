# Utiliser l'image Python 3.13 (ou autre version selon ta préférence)
FROM python:3.13.5-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le contenu du projet
COPY . /app

# Définir la commande par défaut pour exécuter les tests ou le pipeline
# les tests
CMD ["pytest"]

#  exécuter le pipeline
CMD ["python", "src/run_pipeline.py"]
