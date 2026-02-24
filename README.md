# ETL_Partenaire
============================================================
  GUIDE — ETL Recouvrement
============================================================

STRUCTURE DU PROJET
--------------------
recouvrement/
├── create_tables.sql          ← Script SQL (exécuter une seule fois)
├── etl_runner.py              ← Script ETL principal
├── config/
│   └── canvas_types.json      ← ⭐ FICHIER CLÉ (mapping des colonnes)
├── canvas/                     ← Placer les fichiers Excel ici
    |__ archive
└── logs/                      ← Logs automatiques de chaque import


ÉTAPES INITIALES (une seule fois)
-----------------------------------
1. Installer Python :
   https://python.org → Download Python 3.11

2. Installer les librairies :
   Ouvrir CMD et taper :
   pip install pandas pyodbc openpyxl

3. Exécuter create_tables.sql dans SSMS
   → Copier le contenu → Coller dans SSMS → Exécuter (F5)


UTILISATION QUOTIDIENNE
-------------------------
1. Placer le fichier Excel du partenaire dans le dossier canvas/
   ⚠️ Nommer le fichier avec le préfixe du partenaire :
      PARTENAIRE_1_janvier2025.xlsx     ✅
      PARTENAIRE_2_fevrier2025.csv      ✅
      fichier_banque.xlsx               ❌ (pas de préfixe reconnu)

2. Double-cliquer sur etl_runner.py
   ou dans CMD : python etl_runner.py

3. Vérifier les logs dans le dossier logs/


====================================================
  AJOUTER UN NOUVEAU PARTENAIRE (PARTENAIRE 2, 3...)
====================================================

ÉTAPE 1 — Ouvrir canvas_types.json
    Ouvrir le fichier config/canvas_types.json
    avec Notepad ou VS Code

ÉTAPE 2 — Copier le bloc PARTENAIRE_1 et l'adapter
    Ajouter après le bloc PARTENAIRE_1 :

    ,
    "PARTENAIRE_2": {
      "_nom_fichier_exemple": "PARTENAIRE_2_fichier.xlsx",
      "partenaire_nom": "Nom du partenaire 2",
      "partenaire_type": "Ministere",
      "mapping": {
        "company_name":        "NOM_SOCIETE",
        "affiliate_number":    "NUM_AFFILIE",
        "date_adhesion":       "DATE_ADH",
        "date_affiliation":    "DATE_AFF",
        "type_adherent":       "TYPE",
        "company_name_master": "SOCIETE_MERE",
        "affiliate_number_emp":"NUM_EMP",
        "admin_last_name":     "NOM_ADMIN",
        "cin":                 "CIN_ADMIN"
      }
    }

    ⚠️ Les valeurs à DROITE du ":" sont les noms exacts
       des colonnes dans le fichier Excel du partenaire.
       (Copier exactement, respecter majuscules/minuscules)

ÉTAPE 3 — Placer le fichier Excel dans canvas/
    Nommer le fichier : PARTENAIRE_2_xxxxxxx.xlsx

ÉTAPE 4 — Relancer etl_runner.py
    → Le script détecte automatiquement les nouveaux fichiers


EXEMPLES DE MAPPING
--------------------
Si le fichier Excel du partenaire 2 a ces colonnes :
   NOM_SOCIETE | NUM_AFFILIE | DATE_ADH | CIN_ADMIN | ...

Alors dans canvas_types.json :
   "company_name"     : "NOM_SOCIETE"
   "affiliate_number" : "NUM_AFFILIE"
   "date_adhesion"    : "DATE_ADH"
   "cin"              : "CIN_ADMIN"
   etc.


PROBLÈMES FRÉQUENTS
---------------------
❌ Erreur "module not found"
   → Réinstaller : pip install pandas pyodbc openpyxl

❌ Fichier ignoré (pas de préfixe reconnu)
   → Renommer le fichier : PARTENAIRE_1_nom.xlsx

❌ Erreur connexion SQL Server
   → Vérifier que SQL Server est démarré
   → Vérifier le nom du serveur dans etl_runner.py ligne 55

============================================================
