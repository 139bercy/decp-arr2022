# **DECP - Arrêtés du 22 décembre 2022 relatifs aux données essentielles des** **contrats de la commande publique**

Les données essentielles de la commande publique (DECP) font références
à la fusion des données du recensement et des données essentielles.

Les DECP relatives aux marchés publics comprennent 45 données dont 21
conditionnelles (modifications, actes de sous-traitance\...). Elles
doivent être déclarées, pour les marchés dont les montants sont
supérieurs ou égaux à 40 000 € HT. Cette déclaration doit s'effectuer
par l'acheteur (ou par l\'intermédiaire de son profil acheteur ou via
une API ou le PES marché) sur le [portail national des données
ouvertes]{.underline}. Cette déclaration doit s\'effectuer dans les 2
mois à la suite de la notification du contrat. A préciser que cette
obligation peut également concernée les marchés compris entre 25 000 €
et 40 000 €, même si les acheteurs ont cependant la faculté de ne
déclarer que 5 données (objet, montant, durée, nom de l'attributaire et
code postal).

Les DECP relatives aux contrats de concession comprennent 23 données
dont 9 conditionnelles (modifications). Elles doivent être déclarées,
pour tous les contrats de concession. Cette déclaration doit s'effectuer
par l\'autorité concédante (ou par l\'intermédiaire de son profil
acheteur ou via le PES marché) sur le [portail national des données
ouvertes]{.underline}. Cette déclaration doit s\'effectuer avant le
début d\'exécution du contrat.

Les données essentielles de la commande publique sont listées et
définies au sein des arrêtés modifiés du 22/12/2022. Un premier arrêté
concernant les [marchés publics]{.underline} puis un second arrêté
concernant les [contrats de concession]{.underline}.

Afin de permettre la collecte et l\'uniformisation des données, les
fichiers publiés (contenant les DECP) doivent répondre aux exigences des
[schémas de données]{.underline} et s'effectuer aux formats JSON ou XML.

Pour plus d\'information sur les DECP, une [fiche technique]{.underline}
et une [notice]{.underline} sont disponibles.

Afin de faciliter l'accès aux données, les DECP publiées (et répondant
aux exigences des schémas de données) sur le portail national des
données ouvertes sont collectées, agrégées, dédoublonnées et mises à
disposition (format JSON) au sein du jeu de données « [Données
essentielles de la commande publique - fichiers
consolidés ]{.underline}».

Une mise à disposition des données (après retraitements) est également
réalisée sur le site de [l'Open data des Ministères économiques et
financiers]{.underline}.

Les différentes étapes de traitement, s'appliquant aux différentes
sources, sont présentées ci-dessous :

**1. ETAPE GET**

Nous téléchargeons les données d\'une source dans son format d\'origine,
XML ou JSON (les DECP n\'existent pas dans d\'autres formats) dans le
dossier /sources dans un répertoire spécifique à la source des données.

**2. ETAPE CONVERT**

Nous convertissons par la suite en DataFrame afin de faire les
opérations de nettoyage et d\'agrégation.

**3. ETAPE FIX**

Certaines données sources n\'étant pas valides, nous corrigeons ce qui
peut être corrigé (par exemple le format d\'une date). Si certains
champs manquent dans les données, nous avons pris le parti de les garder
et de signaler ces anomalies. On supprime également les lignes
dupliquées (marchés présents plusieurs fois dans la source de données).

**4. ETAPE GLOBAL**

-   **merge_all :** On agrège les DataFrame en un DataFrame unique

-   **drop_duplicate :** On supprime les lignes dupliquées (marchés
    présents dans plusieurs sources de données)

-   **export_to_xml :** On exporte au format XML réglementaire

-   **export_to_json :** On exporte au format JSON réglementaire
