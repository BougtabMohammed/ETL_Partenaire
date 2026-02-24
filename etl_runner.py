"""
============================================================
ETL RUNNER — Recouvrement
============================================================
Ce script lit tous les fichiers Excel/CSV des partenaires
et remplit la base de données SQL Server.

UTILISATION :
    python etl_runner.py

POUR AJOUTER UN NOUVEAU PARTENAIRE :
    → Modifier uniquement config/canvas_types.json
    → Ajouter le fichier Excel dans le dossier canvas/
    → Relancer ce script
============================================================
"""

import os
import json
import logging
import pandas as pd
import pyodbc
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIGURATION LOGGING
# ============================================================
os.makedirs("logs", exist_ok=True)
os.makedirs("canvas/archive", exist_ok=True)
log_file = f"logs/import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ============================================================
# CONNEXION SQL SERVER
# ============================================================
def get_connection():
    """
    Connexion à SQL Server local.
    Modifier SERVER si nécessaire.
    """
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=Recouvrement;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# ============================================================
# CHARGER LA CONFIG CANVAS
# ============================================================
def load_all_configs():
    """Charge tous les types de canvas depuis canvas_types.json"""
    with open("config/canvas_types.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["canvas_types"]


# ============================================================
# PARTENAIRE : créer ou récupérer
# ============================================================
def get_or_create_partenaire(cursor, nom, type_):
    cursor.execute(
        "SELECT id FROM Partenaires WHERE nom = ?", nom
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO Partenaires (nom, type) OUTPUT INSERTED.id VALUES (?, ?)",
        nom, type_
    )
    return cursor.fetchone()[0]


# ============================================================
# ENTREPRISE : créer ou récupérer (évite les doublons)
# ============================================================
def get_or_create_entreprise(cursor, data, partenaire_id):
    """
    Identifiant unique d'une entreprise :
    affiliate_number + partenaire_id
    """
    affiliate = str(data.get("affiliate_number", "")).strip()

    cursor.execute(
        """
        SELECT id FROM Entreprises
        WHERE affiliate_number = ? AND partenaire_id = ?
        """,
        affiliate, partenaire_id
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        """
        INSERT INTO Entreprises
            (company_name, affiliate_number, date_adhesion, date_affiliation,
             type_adherent, company_name_mandataire, affiliate_number_mandataire,
             partenaire_id)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        str(data.get("company_name", "")).strip(),
        str(data.get("affiliate_number", "")).strip(),
        _parse_date(data.get("date_adhesion")),
        _parse_date(data.get("date_affiliation")),
        str(data.get("type_adherent", "")).strip(),
        str(data.get("company_name_mandataire", "")).strip(),
        str(data.get("affiliate_number_mandataire", "")).strip(),
        partenaire_id
    )
    return cursor.fetchone()[0]


# ============================================================
# UTILITAIRES
# ============================================================
def _parse_date(val):
    """Convertit une valeur en date, retourne None si invalide"""
    if pd.isna(val) or val == "" or val is None:
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def lire_fichier(fichier_path):
    """Lit un fichier Excel ou CSV et retourne un DataFrame"""
    ext = Path(fichier_path).suffix.lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(fichier_path, dtype=str)
    elif ext == ".csv":
        return pd.read_csv(fichier_path, dtype=str, encoding="utf-8-sig")
    else:
        raise ValueError(f"Format non supporté : {ext}. Utiliser .xlsx ou .csv")


def appliquer_mapping(df, mapping):
    """
    Renomme les colonnes du fichier Excel
    selon le mapping défini dans canvas_types.json
    """
    # Inverser le mapping : nom_excel → nom_interne
    inverse = {v: k for k, v in mapping.items()}
    df = df.rename(columns=inverse)
    df = df.fillna("")
    return df


# ============================================================
# ETL PRINCIPAL : traiter UN fichier
# ============================================================
def run_etl_fichier(fichier_path, canvas_type, config):
    """
    Importe un fichier Excel/CSV dans SQL Server.

    Args:
        fichier_path : chemin du fichier
        canvas_type  : clé dans canvas_types.json (ex: "PARTENAIRE_1")
        config       : dict de config de ce canvas
    """
    log.info(f"{'='*60}")
    log.info(f"DÉBUT IMPORT : {fichier_path} [{canvas_type}]")

    # Lire le fichier
    try:
        df = lire_fichier(fichier_path)
        log.info(f"{len(df)} lignes détectées dans le fichier")
    except Exception as e:
        log.error(f"Impossible de lire le fichier : {e}")
        return

    # Appliquer le mapping des colonnes
    mapping = config["mapping"]
    df = appliquer_mapping(df, mapping)

    # Connexion DB
    conn = get_connection()
    cursor = conn.cursor()

    # Créer ou récupérer le partenaire
    partenaire_id = get_or_create_partenaire(
        cursor,
        config["partenaire_nom"],
        config["partenaire_type"]
    )
    conn.commit()

    succes = doublons = erreurs = 0

    for index, row in df.iterrows():
        try:
            cin = str(row.get("cin", "")).strip()

            # Vérifier doublon : CIN + partenaire
            if cin:
                cursor.execute(
                    """
                    SELECT id FROM Employes
                    WHERE cin = ? AND partenaire_id = ?
                    """,
                    cin, partenaire_id
                )
                if cursor.fetchone():
                    doublons += 1
                    continue

            # Créer ou récupérer l'entreprise
            entreprise_id = get_or_create_entreprise(
                cursor, row, partenaire_id
            )

            # Insérer l'employé
            cursor.execute(
                """
                INSERT INTO Employes
                    (cin, admin_last_name, entreprise_id, partenaire_id)
                VALUES (?, ?, ?, ?)
                """,
                cin,
                str(row.get("admin_last_name", "")).strip(),
                entreprise_id,
                partenaire_id
            )
            succes += 1

            # Commit par batch de 1000
            if succes % 1000 == 0:
                conn.commit()
                log.info(f"   → {succes} lignes importées...")

        except Exception as e:
            erreurs += 1
            log.error(f"Erreur ligne {index} : {e}")

    conn.commit()
    conn.close()

    log.info(f"""
╔══════════════════════════════════╗
║   RÉSUMÉ : {Path(fichier_path).name:<22} ║
╠══════════════════════════════════╣
║  ✅ Importés   : {succes:<16} ║
║  ⚠️  Doublons  : {doublons:<16} ║
║  ❌ Erreurs    : {erreurs:<16} ║
╚══════════════════════════════════╝""")


# ============================================================
# ARCHIVER UN FICHIER APRÈS TRAITEMENT
# ============================================================
def archiver_fichier(fichier_path):
    """
    Déplace le fichier traité vers archive/ avec horodatage.
    Une fois archivé, le fichier ne sera PLUS jamais retraité.

    Exemple :
      canvas/PARTENAIRE_1_janvier.xlsx
      → archive/PARTENAIRE_1_janvier_20250224_143022.xlsx
    """
    archive_dir = Path("archive")
    archive_dir.mkdir(exist_ok=True)

    fichier = Path(fichier_path)
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    nouveau_nom = f"{fichier.stem}_{horodatage}{fichier.suffix}"
    destination = archive_dir / nouveau_nom

    fichier.rename(destination)
    log.info(f"📦 Archivé → archive/{nouveau_nom}")


# ============================================================
# LANCER TOUS LES FICHIERS DU DOSSIER canvas/
# ============================================================
def run_all():
    """
    Parcourt le dossier canvas/, importe chaque fichier,
    puis le déplace dans archive/ → ne sera plus jamais retraité.

    Convention de nommage :
        canvas/{CANVAS_TYPE}_fichier.xlsx
        Exemple : canvas/PARTENAIRE_1_janvier2025.xlsx
    """
    all_configs = load_all_configs()
    canvas_dir = Path("canvas")

    if not canvas_dir.exists():
        log.error("Dossier 'canvas/' introuvable. Créez-le et placez vos fichiers dedans.")
        return

    fichiers = list(canvas_dir.glob("*.xlsx")) + \
               list(canvas_dir.glob("*.xls")) + \
               list(canvas_dir.glob("*.csv"))

    if not fichiers:
        log.warning("Aucun fichier trouvé dans le dossier canvas/")
        return

    log.info(f"{len(fichiers)} fichier(s) trouvé(s) dans canvas/")

    for fichier in fichiers:
        nom = fichier.stem.upper()
        canvas_type = None

        for key in all_configs.keys():
            if nom.startswith(key):
                canvas_type = key
                break

        if canvas_type is None:
            log.warning(
                f"⚠️  Fichier ignoré (non archivé) : {fichier.name}\n"
                f"   Le nom doit commencer par : {list(all_configs.keys())}\n"
                f"   Exemple : PARTENAIRE_1_janvier.xlsx\n"
                f"   → Corrigez le nom puis relancez."
            )
            # Fichier non reconnu → on ne l'archive PAS
            # Il reste dans canvas/ pour correction manuelle
            continue

        # Traiter le fichier
        run_etl_fichier(
            fichier_path=str(fichier),
            canvas_type=canvas_type,
            config=all_configs[canvas_type]
        )

        # ✅ Archiver après traitement — ne sera JAMAIS retraité
        archiver_fichier(str(fichier))

        # ── Archiver le fichier après import réussi ──────────
        # Le fichier est déplacé dans canvas/archive/
        # avec la date et l'heure dans le nom pour traçabilité
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{fichier.stem}__importé_{timestamp}{fichier.suffix}"
        archive_path = Path("canvas/archive") / archive_name
        fichier.rename(archive_path)
        log.info(f"📦 Fichier archivé → canvas/archive/{archive_name}")


# ============================================================
# POINT D'ENTRÉE
# ============================================================
if __name__ == "__main__":
    log.info("🚀 ETL Recouvrement démarré")
    run_all()
    log.info("✅ ETL terminé")
