-- ============================================================
--  BASE DE DONNÉES : Recouvrement
--  Script de création des tables
--  Basé sur le canvas du premier partenaire
-- ============================================================

CREATE DATABASE Recouvrement;
GO

USE Recouvrement;
GO

-- ============================================================
-- TABLE : Partenaires
-- Chaque partenaire (Banque, Ministère...) est enregistré ici
-- ============================================================
CREATE TABLE Partenaires (
    id               INT PRIMARY KEY IDENTITY,
    nom              NVARCHAR(100) NOT NULL,   -- ex: "Banque X", "Ministère Y"
    type             NVARCHAR(50),             -- "Banque" ou "Ministere"
    date_ajout       DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================
-- TABLE : Entreprises
-- Colonnes issues du canvas :
--   companyName, affiliateNumber, dateAdhesion,
--   dateAffiliation, typeAdherent, companyNameMa
-- ============================================================
CREATE TABLE Entreprises (
    id                   INT PRIMARY KEY IDENTITY,
    company_name                 NVARCHAR(200),    -- companyName (colonne A)
    affiliate_number             NVARCHAR(100),    -- affiliateNumber (colonne B)
    date_adhesion                DATE,             -- dateAdhesion (colonne C)
    date_affiliation             DATE,             -- dateAffiliation (colonne D)
    type_adherent                NVARCHAR(100),    -- typeAdherent (colonne E)
    company_name_mandataire      NVARCHAR(200),    -- companyNameMandataire (colonne F)
    affiliate_number_mandataire  NVARCHAR(100),    -- affiliateNumberMandataire (colonne G)
    partenaire_id                INT NOT NULL FOREIGN KEY REFERENCES Partenaires(id)
);
GO

-- ============================================================
-- TABLE : Employes (admins/contacts dans le canvas)
-- Colonnes issues du canvas :
--   admin_lastName, admin_cin, affiliateNumber (colonne G)
-- ============================================================
CREATE TABLE Employes (
    id                      INT PRIMARY KEY IDENTITY,
    cin                     NVARCHAR(100),    -- admin_cin (colonne I)
    admin_last_name         NVARCHAR(100),    -- admin_lastName (colonne H)
    entreprise_id           INT NOT NULL FOREIGN KEY REFERENCES Entreprises(id),
    partenaire_id           INT NOT NULL FOREIGN KEY REFERENCES Partenaires(id),
    date_import             DATETIME DEFAULT GETDATE()
);
GO

-- ============================================================
-- INDEX : pour performances sur gros volumes (1M+ lignes)
-- ============================================================
CREATE INDEX IX_Employes_CIN
    ON Employes(cin);

CREATE INDEX IX_Employes_Partenaire
    ON Employes(partenaire_id);

CREATE INDEX IX_Employes_Entreprise
    ON Employes(entreprise_id);

CREATE INDEX IX_Entreprises_AffiliateNumber
    ON Entreprises(affiliate_number);

CREATE INDEX IX_Entreprises_Partenaire
    ON Entreprises(partenaire_id);

CREATE INDEX IX_Entreprises_CompanyName
    ON Entreprises(company_name);
GO

-- ============================================================
-- VÉRIFICATION : afficher les tables créées
-- ============================================================
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';
GO
