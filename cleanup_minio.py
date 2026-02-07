#!/usr/bin/env python3
"""
Script pour nettoyer MinIO et garder uniquement les buckets et dossiers nécessaires.

Buckets à CONSERVER complètement :
- iceberg-data/ (tout)
- bronze/ (tout)
- scripts/ (tout)

Buckets avec rétention :
- warehouse/ (garder airflow-logs/ et données Iceberg, SUPPRIMER raw/)
- velero-backups/ (garder backups/ 7-30 jours, restores/ selon besoin)
- db-backups/ (garder postgresql/ 30 jours max)

Buckets à SUPPRIMER :
- Tous les autres buckets non listés ci-dessus
"""
import boto3
import sys
import re
from botocore.client import Config
from datetime import datetime, timedelta

# Configuration MinIO
MINIO_ENDPOINT = "http://minio.minio.svc:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# Buckets à garder complètement (tout le contenu)
BUCKETS_KEEP_ALL = [
    "iceberg-data",
    "bronze",
    "scripts",
]

# Buckets avec nettoyage sélectif
BUCKET_WAREHOUSE = "warehouse"
BUCKET_VELERO = "velero-backups"
BUCKET_DB_BACKUPS = "db-backups"

# Dossiers à garder dans warehouse/ (SUPPRIMER raw/ car données récupérées par DAGs)
WAREHOUSE_KEEP_PREFIXES = [
    "airflow-logs/",  # Tous les logs Airflow
    "weather/",       # Tables Iceberg météo (gérées par Nessie)
    "crypto/",       # Tables Iceberg crypto (gérées par Nessie)
    # Note: raw/ sera SUPPRIMÉ (données récupérées par DAGs)
]

# Rétention Velero
VELERO_BACKUPS_RETENTION_DAYS = 30  # Garder backups 30 jours
VELERO_RESTORES_RETENTION_DAYS = 7  # Garder restores 7 jours

# Rétention DB backups
DB_BACKUPS_RETENTION_DAYS = 30  # Garder backups PostgreSQL 30 jours


def get_minio_client():
    """Créer le client MinIO S3."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        verify=False,
    )


def should_keep_object(key: str, bucket_name: str) -> bool:
    """
    Détermine si un objet doit être gardé selon le bucket et les règles de rétention.
    
    Args:
        key: Clé S3 (chemin de l'objet)
        bucket_name: Nom du bucket
    
    Returns:
        True si l'objet doit être gardé, False sinon
    """
    # Bucket warehouse: garder seulement airflow-logs/ et données Iceberg, SUPPRIMER raw/
    if bucket_name == BUCKET_WAREHOUSE:
        # SUPPRIMER toutes les données raw (récupérées par DAGs)
        if key.startswith("raw/"):
            return False
        
        # Garder les autres dossiers configurés
        for prefix in WAREHOUSE_KEEP_PREFIXES:
            if key.startswith(prefix):
                return True
        
        return False
    
    # Bucket velero-backups: appliquer rétention
    elif bucket_name == BUCKET_VELERO:
        if key.startswith("backups/"):
            return should_keep_by_date(key, VELERO_BACKUPS_RETENTION_DAYS)
        elif key.startswith("restores/"):
            return should_keep_by_date(key, VELERO_RESTORES_RETENTION_DAYS)
        # Garder le reste (structure)
        return True
    
    # Bucket db-backups: appliquer rétention sur postgresql/
    elif bucket_name == BUCKET_DB_BACKUPS:
        if key.startswith("postgresql/"):
            return should_keep_db_backup(key)
        # Garder le reste
        return True
    
    # Pour les autres buckets (iceberg-data, bronze, scripts): tout garder
    return True


def should_keep_by_date(key: str, retention_days: int) -> bool:
    """
    Vérifie si un objet doit être gardé selon sa date et la rétention.
    
    Args:
        key: Clé S3 (peut contenir une date dans le nom)
        retention_days: Nombre de jours de rétention
    
    Returns:
        True si l'objet doit être gardé, False sinon
    """
    try:
        # Essayer d'extraire la date du nom de fichier
        # Formats possibles: YYYY-MM-DD, YYYYMMDD, timestamp, etc.
        
        # Pattern 1: YYYY-MM-DD dans le chemin
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
        if date_match:
            date_str = date_match.group(1)
            obj_date = datetime.strptime(date_str, "%Y-%m-%d")
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            return obj_date >= cutoff_date
        
        # Pattern 2: YYYYMMDD
        date_match = re.search(r'(\d{8})', key)
        if date_match:
            date_str = date_match.group(1)
            obj_date = datetime.strptime(date_str, "%Y%m%d")
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            return obj_date >= cutoff_date
        
        # Pattern 3: Timestamp Unix (10 chiffres)
        timestamp_match = re.search(r'(\d{10})', key)
        if timestamp_match:
            timestamp = int(timestamp_match.group(1))
            obj_date = datetime.fromtimestamp(timestamp)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            return obj_date >= cutoff_date
        
        # Si aucune date trouvée, garder l'objet (sécurité)
        return True
        
    except Exception as e:
        print(f"  ⚠️  Erreur lors de l'analyse de la date pour {key}: {e}")
        # En cas d'erreur, garder l'objet pour sécurité
        return True


def should_keep_db_backup(key: str) -> bool:
    """
    Vérifie si un backup PostgreSQL doit être gardé (30 jours max).
    
    Format attendu: postgresql/{db_name}_YYYY-MM-DD_HHMMSS.sql.gz
    """
    try:
        # Extraire la date du nom de fichier
        # Exemple: postgresql/airflow_db_2026-02-07_120000.sql.gz
        filename = key.split("/")[-1]
        
        # Pattern: {db_name}_YYYY-MM-DD_HHMMSS.sql.gz
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if date_match:
            date_str = date_match.group(1)
            obj_date = datetime.strptime(date_str, "%Y-%m-%d")
            cutoff_date = datetime.now() - timedelta(days=DB_BACKUPS_RETENTION_DAYS)
            return obj_date >= cutoff_date
        
        # Si aucune date trouvée, garder (sécurité)
        return True
        
    except Exception as e:
        print(f"  ⚠️  Erreur lors de l'analyse de la date pour {key}: {e}")
        return True


def cleanup_bucket(s3, bucket_name: str, dry_run: bool = False):
    """
    Nettoie un bucket en gardant uniquement les objets nécessaires.
    
    Args:
        s3: Client boto3 S3
        bucket_name: Nom du bucket
        dry_run: Si True, affiche seulement ce qui serait supprimé sans supprimer
    """
    print(f"\n{'='*60}")
    print(f"Bucket: {bucket_name}")
    print(f"{'='*60}")
    
    # Buckets à garder complètement (pas de nettoyage)
    if bucket_name in BUCKETS_KEEP_ALL:
        print(f"  ✓ Bucket à conserver complètement (pas de nettoyage)")
        return
    
    # Buckets à supprimer complètement (non listés)
    if bucket_name not in [BUCKET_WAREHOUSE, BUCKET_VELERO, BUCKET_DB_BACKUPS] + BUCKETS_KEEP_ALL:
        if dry_run:
            print(f"  [DRY RUN] Suppression complète du bucket: {bucket_name}")
            return
        
        try:
            # Supprimer tous les objets d'abord
            paginator = s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name)
            
            delete_keys = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        delete_keys.append({"Key": obj["Key"]})
            
            if delete_keys:
                # Supprimer par batch de 1000
                for i in range(0, len(delete_keys), 1000):
                    batch = delete_keys[i:i+1000]
                    s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
                print(f"  ✓ Supprimé {len(delete_keys)} objets")
            
            # Supprimer le bucket
            s3.delete_bucket(Bucket=bucket_name)
            print(f"  ✓ Bucket supprimé: {bucket_name}")
        except Exception as e:
            print(f"  ✗ Erreur lors de la suppression du bucket {bucket_name}: {e}")
        return
    
    # Pour warehouse, velero-backups, db-backups: nettoyer sélectivement
    if bucket_name == BUCKET_WAREHOUSE:
        print(f"  Nettoyage sélectif du bucket '{bucket_name}'...")
        print(f"    ✓ Garder: airflow-logs/, weather/, crypto/ (données Iceberg)")
        print(f"    ✗ Supprimer: raw/ (données récupérées par DAGs)")
    elif bucket_name == BUCKET_VELERO:
        print(f"  Nettoyage sélectif du bucket '{bucket_name}'...")
        print(f"    ✓ Garder: backups/ ({VELERO_BACKUPS_RETENTION_DAYS} jours), restores/ ({VELERO_RESTORES_RETENTION_DAYS} jours)")
    elif bucket_name == BUCKET_DB_BACKUPS:
        print(f"  Nettoyage sélectif du bucket '{bucket_name}'...")
        print(f"    ✓ Garder: postgresql/ ({DB_BACKUPS_RETENTION_DAYS} jours max)")
    
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)
    
    total_objects = 0
    kept_objects = 0
    deleted_objects = 0
    to_delete = []
    
    for page in pages:
        if "Contents" not in page:
            continue
        
        for obj in page["Contents"]:
            total_objects += 1
            key = obj["Key"]
            
            if should_keep_object(key, bucket_name):
                kept_objects += 1
                if kept_objects <= 10:  # Afficher les premiers gardés
                    print(f"  ✓ Gardé: {key}")
            else:
                deleted_objects += 1
                to_delete.append({"Key": key})
                if deleted_objects <= 10:  # Afficher les premiers supprimés
                    print(f"  ✗ Supprimé: {key}")
    
    print(f"\n  Résumé:")
    print(f"    Total objets: {total_objects}")
    print(f"    Gardés: {kept_objects}")
    print(f"    À supprimer: {deleted_objects}")
    
    if dry_run:
        print(f"\n  [DRY RUN] Aucune suppression effectuée")
        return
    
    # Supprimer les objets par batch de 1000
    if to_delete:
        print(f"\n  Suppression de {len(to_delete)} objets...")
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i:i+1000]
            try:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
                print(f"    ✓ Supprimé batch {i//1000 + 1} ({len(batch)} objets)")
            except Exception as e:
                print(f"    ✗ Erreur lors de la suppression du batch {i//1000 + 1}: {e}")
        
        print(f"\n  ✓ Nettoyage terminé pour le bucket '{bucket_name}'")


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Nettoyer MinIO et garder uniquement les buckets/dossiers nécessaires")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Afficher ce qui serait supprimé sans supprimer réellement",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirmer la suppression (requis pour exécuter réellement)",
    )
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.confirm:
        print("⚠️  ATTENTION: Ce script va supprimer des données!")
        print("   Utilisez --dry-run pour voir ce qui sera supprimé")
        print("   Utilisez --confirm pour confirmer la suppression")
        print("\n   Exemple:")
        print("     python cleanup_minio.py --dry-run")
        print("     python cleanup_minio.py --confirm")
        sys.exit(1)
    
    print("="*60)
    print("NETTOYAGE MINIO")
    print("="*60)
    print(f"\nConfiguration:")
    print(f"  Endpoint: {MINIO_ENDPOINT}")
    print(f"\n  Buckets à CONSERVER complètement:")
    for bucket in BUCKETS_KEEP_ALL:
        print(f"    ✓ {bucket}/ (tout)")
    print(f"\n  Buckets avec nettoyage sélectif:")
    print(f"    ✓ {BUCKET_WAREHOUSE}/")
    print(f"        - Garder: airflow-logs/, weather/, crypto/ (données Iceberg)")
    print(f"        - Supprimer: raw/ (données récupérées par DAGs)")
    print(f"    ✓ {BUCKET_VELERO}/")
    print(f"        - backups/: {VELERO_BACKUPS_RETENTION_DAYS} jours")
    print(f"        - restores/: {VELERO_RESTORES_RETENTION_DAYS} jours")
    print(f"    ✓ {BUCKET_DB_BACKUPS}/")
    print(f"        - postgresql/: {DB_BACKUPS_RETENTION_DAYS} jours max")
    print(f"\n  Mode: {'DRY RUN' if args.dry_run else 'SUPPRESSION RÉELLE'}")
    print()
    
    if not args.dry_run:
        response = input("⚠️  Confirmez-vous la suppression? (tapez 'yes' pour confirmer): ")
        if response.lower() != "yes":
            print("Annulé.")
            sys.exit(0)
    
    try:
        s3 = get_minio_client()
        
        # Lister tous les buckets
        print("\n📦 Liste des buckets:")
        buckets = s3.list_buckets()
        bucket_names = [b["Name"] for b in buckets["Buckets"]]
        
        if not bucket_names:
            print("  Aucun bucket trouvé")
            return
        
        for bucket_name in bucket_names:
            print(f"  - {bucket_name}")
        
        # Nettoyer chaque bucket
        for bucket_name in bucket_names:
            cleanup_bucket(s3, bucket_name, dry_run=args.dry_run)
        
        print("\n" + "="*60)
        print("NETTOYAGE TERMINÉ")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
