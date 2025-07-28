# decp arrêté du 22 décembre 2022

Voici le lien donnant les informations requises concernant les DECP (les données essentielles de la commande publique) sur [le blog de data.gouv.fr](https://www.data.gouv.fr/fr/posts/le-point-sur-les-donnees-essentielles-de-la-commande-publique/).

Les données aggrégées sont publiées ici : (**[jeu de données sur data.gouv.fr](https://www.data.gouv.fr/fr/datasets/5cd57bf68b4c4179299eb0e9)**) aux formats JSON et XML réglementaires.

La procédure standard est la suivante (pour chaque source de données) :

### **1. ETAPE GET**

Nous téléchargeons les données d'une source dans son format d'origine, XML ou JSON (les DECP n'existent pas dans d'autres formats) dans le dossier /sources dans un répertoire spécifique à la source des données.

### **2. ETAPE CONVERT**

Nous convertissons par la suite en DataFrame afin de faire les opérations de nettoyage et d'aggrégation.

### **3. ETAPE FIX**

Certaines données sources n'étant pas valides, nous corrigeons ce qui peut être corrigé (par exemple le format d'une date). Si certains champs manquent dans les données, nous avons pris le parti de les garder et de signaler ces anomalies. On supprime également les lignes dupliquées (marchés présents plusieurs fois dans la source de données).

### **4. ETAPE GLOBAL**

- **merge_all :** On agrège les DataFrame en un DataFrame unique
- **drop_duplicate :** On supprime les lignes dupliquées (marchés présents dans plusieurs sources de données)
- **export_to_xml :** On exporte au format XML réglementaire
- **export_to_json :** On exporte au format JSON réglementaire
