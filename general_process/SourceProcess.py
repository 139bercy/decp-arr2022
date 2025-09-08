from __future__ import annotations
import linecache
from xml.etree import ElementTree
import wget
import ssl
import urllib.request, ssl, certifi
import os
import json
import xml
import xmlschema
import jsonschema
from jsonschema import validate,Draft7Validator,Draft202012Validator
from lxml import etree
from datetime import datetime
from pypdl import Pypdl
import pandas as pd
import numpy as np
import xmltodict
import dict2xml
import re
import logging
import shutil
from urllib.parse import urlparse
import csv
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)
from reporting.Report import Report
from utils.NodeFormat import NodeFormat
from utils.StepMngmt import StepMngmt
from utils.Step import Step
import re

class SourceProcess:

    """La classe SourceProcess est une classe abstraite qui sert de parent à chaque classe enfant de
    sources. Elle sert à définir le cas général des étapes de traitement d'une source : création des
    variables de classe (__init__), nettoyage des dossiers de la source (_clean_metadata_folder),
    récupération des URLs (_url_init), get, convert et fix."""
    
    def __init__(self, key, data_format, report:Report):
        """L'étape __init__ crée les variables associées à la classe SourceProcess : key, source,
        format, df, title, url, cle_api et metadata.
        
        Args:
            key: clé qui permettant d'identifier la source du processus
            data_format: il s'agit de l'année 2022 ou 2019
        """
        logging.info("--- ÉTAPE INIT")
        self.report = report
        self.key = key
        self.data_format = data_format
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.url_source = self.metadata[self.key]["url_source"]
        self.date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        self.date_pattern_year = re.compile(r'-20\d{2}.')
        self.date_pattern_inv = re.compile(r'\d{2}\.{1}\d{2}\.{1}\d{4}')
            
        self.validate = self.metadata[self.key]["validate"]
        self.convert_nc = self.metadata[self.key]["convert_nc"]
        self.df = pd.DataFrame()
        self.title = []
        # Lavage des dossiers de la source
        self._clean_metadata_folder()

        # Récupération des urls
        self._url_init() 

        # Liste des dictionnaires pour l'étape de nettoyage
        self.dico_2022_marche = []
        self.dico_2022_concession = []

        # Chargement du schemas json
        scheme_path = 'schemes/schema_decp_v2.0.3.json'
        with open(scheme_path, "r",encoding='utf-8') as json_file:
            self.json_scheme = json.load(json_file)
            json_file.close

    
    def _clean_metadata_folder(self) -> None:
        """La fonction _clean_metadata_folder permet le nettoyage de /metadata/{self.source}"""
        # Lavage des dossiers dans metadata
        logging.info(f"Début du nettoyage de metadata/{self.source}")
        logging.info(os.path.abspath('.'))
        if os.path.exists(f"metadata/{self.source}"):
            shutil.rmtree(f"metadata/{self.source}")
        logging.info(f"Nettoyage metadata/{self.source} OK")


    def _url_init(self) -> None:
        """_url_init permet la récupération de l'ensemble des url des fichiers qui doivent être
        téléchargés pour une source. Ces urls sont conservés dans self.metadata."""

        logging.info("Initialisation")
        os.makedirs(f"metadata/{self.source}", exist_ok=True) 
        os.makedirs(f"old_metadata/{self.source}", exist_ok=True)
        self.cle_api = self.metadata[self.key]["cle_api"]
        #Liste contenant les urls à partir desquels on télécharge les fichiers
        if self.cle_api==[]:
            self.url = [self.url_source]
        else:
            self.url, self.title = self._create_metadata_file(len(self.cle_api))
        
        logging.info(" ")
        logging.info("Initialisation finie")
    
    @staticmethod
    def _date_in_intervale(date_txt:str, date_begin, date_end) -> bool:
        # Conversion des chaînes de caractères en objets Date
        date_a_verifier = datetime.strptime(date_txt, "%d.%m.%Y")

        # Vérification de la séquence logique
        if date_begin <= date_a_verifier <= date_end:
            return True  # La date est comprise entre les deux autres dates.
        else:
            return False

    def _create_metadata_file(self,n:int)->tuple[list,list]:
        """
        Fonction réalisant le téléchargement des métadatas, la copie des
        fichiers métadatas et la création des listes contenant les titres
        et les urls des fichiers.
        
        Args: 

            n: nombre de clé api 

        """
        logging.info("Début de la récupération de la liste des urls")
        title = []  
        url = []    
        for i in range(n):
            #Téléchargement du fichier de metadata de self.source et création de la 1ere variable json pour la comparaison 
            try:
                # Replaced after certifi can't validate ssl certificat
                wget.download(f"https://www.data.gouv.fr/api/1/datasets/{self.cle_api[i]}/",
                            f"metadata/{self.source}/metadata_{self.key}_{i}.json")
                #url = f"https://www.data.gouv.fr/api/1/datasets/{self.cle_api[i]}/"
                #context = ssl.create_default_context(cafile=certifi.where())
                #with urllib.request.urlopen(url, context=context) as response, open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'wb') as out_file:
                #    out_file.write(response.read())
            except Exception as err:
                logging.error("Erreur lors du chargement des métadonnées")
                logging.error(err)

            with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r+') as f:
                ref_json = json.load(f)
            ressources = ref_json["resources"]

            #Creation de la deuxième variable json pour la comparaison si le fichier old_metadata existe
            if os.path.exists(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json"):
                with open(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json", 'r+') as fl:
                    refjson = json.load(fl)
                old_ressources = refjson["resources"]
            else:
                old_ressources = []

            # Quand plusieurs api_key sont déclarées on ajoute un prefixe 
            # pour eviter d'écraser les fichiers ayant le même nom
            if n>1:
                prefix = str(i)+'_'+self.cle_api[i]+'_'
            else:
                prefix = ''
            #Création de la liste des urls selon 2 cas: 1er téléchargement, i-nième téléchargement
            if old_ressources==[]:
                url = url + [d["url"] for d in ressources if
                            (d["url"].endswith("xml") or d["url"].endswith("json"))]
                title = title + [prefix+d["title"] for d in ressources]
            else: 
                url, title = self.check_date_file(url,title, ressources, old_ressources,prefix)
            

            ## Code for generate all files for months and year between given dates 
            # Filter by date in title, url
            """
            begin_date_txt = "2024-01-01"
            end_date_txt = "2024-12-31"
            begin_date = datetime.strptime(begin_date_txt, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_txt, "%Y-%m-%d")
            
            filtered_url = []
            filtered_title = []
            for u, t in zip(url, title):
                match = self.date_pattern.search(u)
                if match:
                    file_date = match.group() #[1:5]+"-01-01"
                    if begin_date_txt <= file_date <= end_date_txt:
                        filtered_url.append(u)
                        filtered_title.append(t)
                else:
                    match = self.date_pattern_inv.search(u)
                    if match:
                        file_date = match.group()
                        if SourceProcess._date_in_intervale(file_date, begin_date,end_date):
                            filtered_url.append(u)
                            filtered_title.append(t)
                    else:
                        if "-2024" in t or "-2025" in t:
                            # Date not found in url, we keep the file for further analysis
                            filtered_url.append(u)
                            filtered_title.append(t)
            url = filtered_url
            title = filtered_title
            """

            #Cas où les fichiers old_metadata existent: on écrit dedans à nouveau
            if os.path.exists(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json"):
                with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r') as source_file:
                    contenu = source_file.read()
                with open(f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json", 'w') as destination_file:
                    destination_file.write(contenu)
            #Cas où les fichiers old_metadata n'existent pas: on fait une copie
            else:
                shutil.copy(f"metadata/{self.source}/metadata_{self.key}_{i}.json",f"old_metadata/{self.source}/old_metadata_{self.key}_{i}.json")
                logging.info(os.listdir(f"old_metadata/{self.source}"))

        return url,title


    def check_date_file(self,url:list, title: list, new_ressources:dict,old_ressources:dict,prefix:str)->tuple[list,list]:
        """
        Fonction vérifiant si la date de dernière modification des fichiers ressources 
        dans les metadatas est strictement antérieure à la date de dernière modification.

        Args:

            url: la liste contenant les liens pour télécharger les fichiers
            new_ressources: dictionnaire correspondant au champ "resources" dans le fichier metadata de la source
            old_ressources: dictionnaire correspondant au champ "resources" dans le fichier old_metadata de la source

        """
        # Creation de la liste des fichiers à traiter
        # On inclus les fichiers qui n ont pas été traités dans une session précédente
        # et ceux dont la date de publication est postérieure a celle memorisee dans old_metadata
        old_urls = {d['url'] for d in old_ressources}
        for d in new_ressources:
            if (d["url"].endswith("xml") or d["url"].endswith("json")):
                if d['url'] not in old_urls or d['last_modified'] > next((item['last_modified'] for item in old_ressources if item['url'] == d['url']), None):
                    url = url + [d["url"]] 
                    title = title + [prefix+d["title"]]

        return url, title         
    

    def get(self) -> None:
        """
        Étape get qui permet le lavage du dossier sources/{self.source} et 
        la récupération de l'ensemble des fichiers présents sur chaque url.
        """
        logging.info("--- ÉTAPE GET")
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s)")
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        if self.cle_api==[]:
            logging.info("Pas de clé api pour télécharger les données")
            self._download_without_metadata()
        else:
            # Verification de l'existence d'un eventuel doublon + nettoyage + 
            # Téléchargement du nouveau fichier
            dl = Pypdl(allow_reuse=True)
            for i in range(len(self.url)):
                try:
                    if os.path.exists(f"sources/{self.source}/{self.title[i]}"):
                        os.remove(f"sources/{self.source}/{self.title[i]}")
                        logging.info(f"Fichier : {self.title[i]} existe déjà, nettoyage du doublon ")
                    #wget.download(self.url[i], f"sources/{self.source}/{self.title[i]}")
                    dl.start(url=self.url[i],file_path=f"sources/{self.source}/{self.title[i]}",retries=10,display=False)
                except:
                    logging.error(f"Problème de téléchargement du fichier {self.url[i]}")
        logging.info(f"Téléchargement : {len(self.url)} fichier(s) OK")


    def _download_without_metadata(self) -> None:
        """
        Fonction téléchargeant un fichier n'ayant pas de clé api. Par 
        conséquent, le téléchargement s'effectue grâce à l'url dans 
        l'attribut url_source
        """
        nom_fichiers = os.listdir(f"sources/{self.source}")
        parsed_url = urlparse(self.url[0])

        # Obtenir le chemin de l'URL et extraire le nom du fichier
        file_name = parsed_url.path.split('/')[-1]

        if nom_fichiers!=[]:   #Dossier non vide
            os.remove(f"sources/{self.source}/{nom_fichiers[0]}")
            logging.info(f"Le fichier {nom_fichiers[0]} était présent. Il a été supprimé")
            
            # Replaced after certifi can't validate ssl certificat
            wget.download(self.url[0], f"sources/{self.source}/{file_name}")
            #url = self.url[0]
            #context = ssl.create_default_context(cafile=certifi.where())
            #with urllib.request.urlopen(url, context=context) as response, open(f"sources/{self.source}/{file_name}", 'wb') as out_file:
            #    out_file.write(response.read())

            self.title = [ file_name ]
            logging.info(f"Titre des fichiers : {self.title}")

        #Le dossier est vide car il s'agit du 1er téléchargement. Téléchargement 
        #dans le dossier puis affectation du nom du fichier à l'attribut titre
        else:
            # Replaced after certifi can't validate ssl certificat
            wget.download(self.url[0], f"sources/{self.source}/")
            #url = self.url[0]
            #context = ssl.create_default_context(cafile=certifi.where())
            #with urllib.request.urlopen(url, context=context) as response, open(f"sources/{self.source}/", 'wb') as out_file:
            #    out_file.write(response.read())

            logging.info(os.listdir(f"sources/{self.source}"))
            self.title = [ os.listdir(f"sources/{self.source}")[0] ]
            logging.info(f"Titre des fichiers : {self.title}")


    def clean(self) -> None:
        """
        Cette fonction extrait les dictionnaires des fichiers 
        (suivant le format 2022) pour qu'ils puissent être nettoyés.
        Grâce à la fonction validation_format, une sélection est effectuée sur ces
        dictionnaires pour séparer les marchés et les concessions respectant le format 
        des "mauvais".
        """   
        logging.info("--- ÉTAPE CLEAN")
        logging.info("Début du nettoyage des nouveaux fichiers")
        #Ouverture des fichiers
        dico = {}

        # Tests AIFE limite url
        #self.url = [f"sources/{self.source}/decp-13000495500139-2025-05-05-02_.xml"]
        #self.url += [f"sources/{self.source}/decp-13000495500139-2025-05-05-02_.xml"]
        #self.title = ["decp-13000495500139-2025-05-05-02_.xml"]
        #self.title += ["Donnees-Essentielles-Marches13.03.2025.11-50.xml"]
        # Force title force url file list
        #self.title = os.listdir("sources\\xmarches")

        for i in range(len(self.title)):            
            if self.format == 'xml':
                try:
                    with open(f"sources/{self.source}/{self.title[i]}", encoding='utf-8') as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict, \
                            force_list=('marche','contrat-concession',
                                'titulaires','donneesExecution','modifications',
                                'actesSousTraitance','modificationsActesSousTraitance'))
                                #'techniques','typesPrix','modalitesExecution',
                                #'considerationsEnvironnementales','considerationsSociales'))
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier xml {self.title[i]} - {err}")

                if 'marches' in dico and 'marche' in dico['marches']:
                    j = 0
                    for marche in dico['marches']['marche']:
                        if self.convert_nc:
                            NodeFormat.force_bools_nc(['sousTraitanceDeclaree','marcheInnovant','attributionAvance'],marche)
                            NodeFormat.force_floats_nc(['tauxAvance','origineUE','origineFrance','montant'],marche)
                            NodeFormat.force_ints_nc(['offresRecues','dureeMois'],marche)
                        else:
                            NodeFormat.force_bools(['sousTraitanceDeclaree','marcheInnovant','attributionAvance'],marche)
                            NodeFormat.force_floats(['tauxAvance','origineUE','origineFrance','montant'],marche)
                            NodeFormat.force_ints(['offresRecues','dureeMois'],marche)

                        if 'titulaires' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'titulaires', 'titulaire'):
                            NodeFormat.normalize_list_node(marche,'titulaires', 'titulaire')

                        if 'concessionnaires' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'concessionnaires', 'concessionnaire'):
                            NodeFormat.normalize_list_node(marche,'concessionnaires', 'concessionnaire')
                        
                        if 'donneesExecution' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'donneesExecution', 'donneesAnnuelles'):
                            NodeFormat.normalize_list_node(marche,'donneesExecution', 'donneesAnnuelles')

                        if 'modifications' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'modifications', 'modification'):
                            NodeFormat.normalize_list_node(marche,'modifications', 'modification')
                        NodeFormat.convert_ints(marche,'modifications', 'modification')
                        NodeFormat.normalize_list_node_inside(marche,'titulaires','titulaire','modifications', 'modification')
                        

                        if self.format == "xml":
                            if 'modificationsActesSousTraitance' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'modificationsActesSousTraitance', 'modificationActesSousTraitance'):
                                NodeFormat.normalize_list_node(marche,'modificationsActesSousTraitance', 'modificationActesSousTraitance')
                            NodeFormat.convert_ints(marche,'modificationsActesSousTraitance', 'modificationActeSousTraitance')
                        else:
                            if 'modificationsActesSousTraitance' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'modificationsActesSousTraitance', 'modificationActeSousTraitance'):
                                NodeFormat.normalize_list_node(marche,'modificationsActesSousTraitance', 'modificationActeSousTraitance')
                            NodeFormat.convert_ints(marche,'modificationsActesSousTraitance', 'modificationActeSousTraitance')

                        if 'actesSousTraitance' in marche.keys() and not NodeFormat.is_normalized_list_node(marche,'actesSousTraitance', 'acteSousTraitance'):
                            NodeFormat.normalize_list_node(marche,'actesSousTraitance', 'acteSousTraitance')
                        NodeFormat.convert_ints(marche,'actesSousTraitance', 'acteSousTraitance')
                        
                        if 'modalitesExecution' in marche.keys() and not NodeFormat.is_normalized_list_value(marche,'modalitesExecution', 'modaliteExecution'):
                            NodeFormat.normalize_list_value(marche,'modalitesExecution', 'modaliteExecution')

                        if 'techniques' in marche.keys() and not NodeFormat.is_normalized_list_value(marche,'techniques', 'technique'):
                            NodeFormat.normalize_list_value(marche,'techniques', 'technique')

                        if 'typesPrix' in marche.keys() and not NodeFormat.is_normalized_list_value(marche,'typesPrix', 'typePrix'):
                            NodeFormat.normalize_list_value(marche,'typesPrix', 'typePrix')
                            
                        if 'considerationsSociales' in marche.keys() and not NodeFormat.is_normalized_list_value(marche,'considerationsSociales', 'considerationSociale'):
                            NodeFormat.normalize_list_value(marche,'considerationsSociales', 'considerationSociale')
                            
                        if 'considerationsEnvironnementales' in marche.keys() and not NodeFormat.is_normalized_list_value(marche,'considerationsEnvironnementales', 'considerationEnvironnementale'):
                            NodeFormat.normalize_list_value(marche,'considerationsEnvironnementales', 'considerationEnvironnementale')
                        j += 1

            elif self.format == 'json':
                try:
                    with open(f"sources/{self.source}/{self.title[i]}", encoding="utf-8") as json_file1:
                        dico = json.load(json_file1)
                except Exception as err:
                    logging.error(f"Exception lors du chargement du fichier json {self.title[i]} - {err}")
            try:
                self._validation_format(dico['marches'], self.title[i])    #On obtient 2 fichiers qui sont mis jour à chaque tour de boucle
            except Exception as err:
                logging.error(f"Exception lors de la validation du format des données: {err}")

        logging.info("Fin du nettoyage des nouveaux fichier")

    def _get_year_month(self,file_name):
        year_month = None
        d = re.search(self.date_pattern, file_name)
        if d is not None:
            try:
                year_month = pd.to_datetime(d.group()).strftime('%Y-%m')
            except Exception:
                year_month = None
        else:
            d = re.search(self.date_pattern_inv, file_name)
            if d is not None:
                try:
                    year_month = pd.to_datetime(d.group(),format="%d.%m.%Y").strftime('%Y-%m')
                except Exception:
                    year_month = None
        return year_month
    
    def _validation_format(self, dico:dict, file_name:str) -> None:
        """
        Cette fonction permet de vérifier la structure du dictionnaire fournit en
        entrée. Si le schéma est respecté, les marchés et concessions correctes
        sont stockés dans des listes et des fichiers pour une future utilisation.
        Il en est de même pour les marchés et les concessions incorrectes.

        Args:

            dico : il s'agit d'un dictionnaire avec 2 clés: 'marchés' et 'contrat-concession'
            file_name : nom du fichier où se trouve le dictionnaire dico

        """
        def complete_util_info(rec,source,file_name,year_month,position,error_message,error_path):
            # Adding source and file_name for reporting
            rec['report__file'] = file_name
            if source not in rec:
                rec['source'] = source
            rec['report__position'] = position
            if error_message is not None:
                rec['report__error'] = error_message
            if error_path is not None:
                rec['report__path'] = error_path
            rec['tmp__annee_mois'] = year_month
            return rec

        # Get year-month suffix for this data set for merging data in export
        year_month = self._get_year_month(file_name)

        nb_total_marches,nb_total_concessions = self.get_nb_enregistrements(dico);

        # On mémorise le fichier source et les nombres de marchés et de concession
        self.report.db_add_file(self.source,file_name,nb_total_marches,nb_total_concessions)

        logging.info(f"Nombre de marchés et concessions à valider dans {file_name}: {(nb_total_marches+nb_total_concessions)} ")

        draft_validator = Draft7Validator(self.json_scheme)
        n, m = 0, 0
        nb_good_marches,nb_good_concessions = 0, 0
        dico_ignored_marche, dico_ignored_concession = [], []
        error_message = None
        aucun_marches = False

        local_source = None

        if 'marche' in dico and isinstance(dico['marche'],list):
            while n < len(dico['marche']) :
                #self.dico_2022_marche.append(dico['marche'][n])
                if dico['marche'][n] is not None:
                    if 'source' in dico['marche'][n]:
                        local_source = dico['marche'][n]["source"]
                        del dico['marche'][n]["source"]
                    else:
                        local_source = None
                    dico_test = {'marches': {'marche': [dico['marche'][n]], 'contrat-concession': []}}

                    valid,error_message,error_path = self.check_json_batch(dico_test,draft_validator)
                    if self.validate and not valid:
                        #self.dico_2022_marche.remove(dico['marche'][n])
                        dico_ignored_marche.append(complete_util_info(dico['marche'][n],self.source,file_name,year_month,n,error_message,error_path))
                    else: 
                        self.dico_2022_marche.append(complete_util_info(dico['marche'][n],self.source if local_source is None else local_source,file_name,year_month,n,error_message,error_path))
                        nb_good_marches+=1
                n+=1
        elif 'marche' in dico:
            dico_ignored_concession.append(complete_util_info(dico['marche'],self.source,file_name,year_month,0,'Une liste de marchés est attendue',''))
        else:
            aucun_marches = True

        # Mise a jour du nombre de marchés ignorés a    
        self.report.nb_in_bad_marches += len(dico_ignored_marche)
        self.report.nb_in_good_marches += nb_good_marches

        if 'contrat-concession' in dico and isinstance(dico['contrat-concession'],list):
            while m < len(dico['contrat-concession']) :
                #self.dico_2022_concession.append(dico['contrat-concession'][m])
                if dico['contrat-concession'][m] is not None:
                    if 'source' in dico['contrat-concession'][m]:
                        local_source = dico['contrat-concession'][m]["source"]
                        del dico['contrat-concession'][m]["source"]
                    else:
                        local_source = None
                    dico_test = {'marches': {'marche': [], 'contrat-concession': [dico['contrat-concession'][m]]}}

                    valid,error_message,error_path = self.check_json(dico_test)
                    if self.validate and not valid:
                        #self.dico_2022_concession.remove(dico['contrat-concession'][m])
                        dico_ignored_concession.append(complete_util_info(dico['contrat-concession'][m],self.source,file_name,year_month,m,error_message,error_path))
                    else: 
                        self.dico_2022_concession.append(complete_util_info(dico['contrat-concession'][m],self.source if local_source is None else local_source,file_name,year_month,m,error_message,error_path))
                        nb_good_concessions+=1
                m+=1
        elif 'contrat-concession' in dico:
            dico_ignored_concession.append(complete_util_info(dico['contrat-concession'],self.source,file_name,year_month,0,'Une liste de concessions est attendue',''))
        elif aucun_marches:
            self.report.db_add_error_file('Clean',self.report.E_VALIDATION,self.source,file_name,'Aucun marchés ni concessions n\'ont été retrouvé dans le fichier')

        # Mise a jour du nombre de concessions ignorées  
        self.report.nb_in_bad_concessions += len(dico_ignored_concession)
        self.report.nb_in_good_concessions += nb_good_concessions

        # Structure du nouveau fichier JSON, création des dictionnaires valides et invalides
        # obsolete jsonfile = {'marches': {'marche':  dico_ignored_marche, 'contrat-concession': dico_ignored_concession}}

        if len(dico_ignored_marche)>0:
            self.report.add('Clean/Marchés',self.report.E_VALIDATION,'Marché non valide',dico_ignored_marche)
        if len(dico_ignored_concession)>0:
            self.report.add('Clean/Concession',self.report.E_VALIDATION,'Concession non valide',dico_ignored_concession)
        
        # Si la source est ajouté sans validation on mémorise quamd même les erreurs
        if not self.validate: 
            if len(self.dico_2022_marche)>0:
                self.report.add_forced('Clean/Marchés',self.report.E_VALIDATION,'Marché non valide mais ajouté',self.dico_2022_marche)
            if len(self.dico_2022_concession)>0:
                self.report.add_forced('Clean/Concession',self.report.E_VALIDATION,'Concession non valide mais ajoutée',self.dico_2022_concession)

        logging.info(f"Nombre de marchés et concessions valides dans {file_name}:   {nb_good_marches+nb_good_concessions} (ignorés: {len(dico_ignored_marche)+len(dico_ignored_concession)})")


    def _add_column_type(self, df: pd.DataFrame, default_type_name:str = None) -> None :
        """
        La fonction ajoute une colonne "_type" dans le dataframe
        dont les valeurs seront: "Marché" ou "Concession". 

        Args: 

            df: dataframe à compléter
            default_type_name: il s'agit de l'une des 2 valeurs possibles

        """
        if self.data_format=='2022' and "_type" not in df.columns and (default_type_name or "nature" in df.columns):
            #if default_type_name:
            if default_type_name:
                df['_type'] = default_type_name
            else:
                df['_type'] = df["nature"].apply(lambda x: "Marché" if "march" in x.lower() else "Concession")


    def convert(self) -> None:
        """
        Étape de conversion des fichiers qui concatène les fichiers présents dans 
        {self.source} dans un seul DataFrame. Elle utilise le dictionnaire 
        des marchés/concessions valides de chaque fichier pour le convertir en un 
        dataframe. L'ensemble des dataframes est stocké dans une liste. 
        """
        logging.info("--- ÉTAPE CONVERT")
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")

        #Liste qui conservera les dataframes. 
        li = []

        # Ajout d'un marché à la liste des dataframes
        df = pd.DataFrame.from_dict(self.dico_2022_marche)
        if "_type" not in df.columns:
            self._add_column_type(df,"Marché")
        li.append(df)

        # Ajout d'une concession à la liste des dataframes
        df = pd.DataFrame.from_dict(self.dico_2022_concession)
        if "_type" not in df.columns:
            self._add_column_type(df,"Concession")
        li.append(df)

        #Concaténation des dataframes de la liste li en un seul dataframe                  
        if len(li) != 0:
            df = pd.concat(li)
            df = df.reset_index(drop=True)
        else:
            # create empty dataframe
            df = pd.DataFrame()
        self.df = df

        logging.info("Conversion OK")
        logging.info(f"Nombre de marchés/concessions dans {self.source} après convert : {len(self.df)}")


    def _validate_json(self, json_data:dict,json_scheme:dict) -> tuple[bool,str,str]:
        """
        Fonction vérifiant si le fichier jsn "json_data" respecte
        le schéma spécifié dans le  schéma en paramètre "json_scheme". 

        Args: 

            json_data: dictionnaire qui va être vérifié par le validateur
            json_scheme: schéma à respecter

        """
        try:
            # Draft7Validator.check_schema(jsonScheme)
            # Draft202012Validator.check_schema(jsonScheme)
            validate(instance=json_data, schema=json_scheme)
        except jsonschema.exceptions.ValidationError as err: 
            #logging.error(f"Erreur de validation json - {err.message}")
            return False, err.message, err.json_path
        return True, None, None

    def _validate_json_batch(self, json_data:dict,draft_validator:Draft7Validator) -> tuple[bool,str,str]:
        """
        Fonction vérifiant si le fichier jsn "json_data" respecte
        le schéma spécifié dans le  schéma en paramètre "json_scheme". 

        Args: 

            json_data: dictionnaire qui va être vérifié par le validateur
            draft_validator: Instance du validator initialisee avec le schéma à respecter

        """
        try:
            # Draft7Validator.check_schema(jsonScheme)
            # Draft202012Validator.check_schema(jsonScheme)
            draft_validator.validate(instance=json_data)
        except jsonschema.exceptions.ValidationError as err: 
            #logging.error(f"Erreur de validation json - {err.message}")
            return False, err.message, err.json_path
        return True, None, None


    def check_json(self,json_data) -> tuple[bool,str,str]:
        """
        Fonction qui prend en paramètre une donnée json
        et vérifiant, grâce à un schéma, que la donnée est valide.

        Args:

            json_data : donnée json en entrée

        """
        return self._validate_json(json_data,self.json_scheme)
    
    def check_json_batch(self,json_data,draft_validator:Draft7Validator) -> tuple[bool,str,str]:
        """
        Fonction qui prend en paramètre une donnée json
        et vérifiant, grâce à un schéma, que la donnée est valide.

        Args:

            json_data : donnée json en entrée

        """
        return self._validate_json_batch(json_data,draft_validator)
    

    def convert_boolean_DEPRECATED(self,col_name:str) -> None:
        """
        Permet de remplacer les valeurs booléennes "Vrai" ou "Faux" par "oui" ou "non"

        Args:

            col_name: colonne où s'effectue le changement

        """
        #Conversion si il s'agit de string
        if self.df[col_name].dtypes == 'object':  
            self.df[col_name] = self.df[col_name].astype(str).replace({'1': 'oui', 'true': 'oui', '0': 'non', 'false': 'non','True': 'oui', 'False': 'non'})
        else:
            self.df[col_name] = self.df[col_name].astype(str).replace({'True': 'oui', 'False': 'non' }) 

    def convert_boolean(self,col_name:str) -> None:
        """
        Permet de remplacer les valeurs booléennes "1" ou "0", "Vrai" ou "Faux", "True" ou "False" par True ou False

        Args:

            col_name: colonne où s'effectue le changement

        """
        #Conversion si il s'agit de string
        if self.df[col_name].dtypes == 'object':
            #self.df[col_name] = self.df[col_name].astype(str).replace({'1': True, 'true': True, 'True': True, '0': False, 'false': False, 'False': False})
            with pd.option_context("future.no_silent_downcasting", True):
                self.df["backup__"+col_name] = self.df[col_name]
                self.df[col_name] = self.df[col_name].replace({'1': True, 'true': True, 'True': True, '0': False, 'false': False, 'False': False}).infer_objects(copy=False)
        else:
            #self.df[col_name] = self.df[col_name].astype(str).replace({'True': True, 'False': False })
            with pd.option_context("future.no_silent_downcasting", True):
                self.df["backup__"+col_name] = self.df[col_name]
                self.df[col_name] = self.df[col_name].replace({'True': True, 'False': False }).infer_objects(copy=False)
        #self.df[col_name] = self.df[col_name].astype(bool)

    def fix(self) -> None:
        """
        Étape fix qui crée la colonne source dans le
        DataFrame et qui supprime les doublons purs.
        """
        def check_dico(dico):
            #Prend en entrée le dictionnaire du champ "acheteur"
            if dico is not np.nan and (dico=={} or dico is None or dico['id']==None):
                return True
            return False
        
        def update_id(ligne):
            #Modifie le champ "id" du champ acheteur
            if check_dico(ligne["acheteur"]):
                ligne["acheteur"] = {"id": ligne["id"] }
            return ligne      
        
        def tri_titulaires(titulaires):
            # return sorted(titulaires, key=lambda x: x['titulaire']['id']) if isinstance(titulaires, list) else titulaires
            # Utiliser sorted si titulaires est une liste
            if isinstance(titulaires, list):
                return sorted([t for t in titulaires if 'id' in t['titulaire']], key=lambda x: x['titulaire']['id'])
            # Si titulaires est un dict (par exemple, un dataframe converti en dict), on traite différemment
            elif isinstance(titulaires, dict):
                # Filtrer et trier les entrées qui ont bien l'attribut 'id'
                return {k: v for k, v in titulaires.items() if 'id' in v and 'titulaire' in v and 'id' in v['titulaire']}
            # else:
            #     raise TypeError("L'entrée titulaires doit être une liste ou un dictionnaire.")

        def tri_concessionnaires(concessionnaires):
            return sorted(concessionnaires, key=lambda x: x['concessionnaire']['id']) if isinstance(concessionnaires, list) else concessionnaires

        logging.info("--- ÉTAPE FIX")
        logging.info(f"Début de fix: Ajout source et suppression des doublons de {self.source}")
        # Ajout de source
        #self.df = self.df.assign(source=self.source)

        # Application de la fonction de tri
        if 'titulaires' in self.df.columns:
            self.df['titulaires'] = self.df['titulaires'].apply(tri_titulaires)
        if 'concessionnaires' in self.df.columns:
            self.df['concessionnaires'] = self.df['concessionnaires'].apply(tri_concessionnaires)

        # Pour les flux en exception avec "NC" ## OBSOLETE on duplique les colonnes qui contiendront des NC 
        # et on converti les "NC" en Nan
        if self.convert_nc:
            self.enlever_nc_colonne(self.df,'offresRecues')
            self.enlever_nc_colonne(self.df,'marcheInnovant')
            self.enlever_nc_colonne(self.df,'attributionAvance')
            self.enlever_nc_colonne(self.df,'sousTraitanceDeclaree')
            self.enlever_nc_colonne(self.df,'dureeMois')
            self.enlever_nc_colonne(self.df,'variationPrix')
            self.enlever_nc_colonne_inside(self.df,'dureeMois','actesSousTraitance','acteSousTraitance')
            self.enlever_nc_colonne_inside(self.df,'variationPrix','actesSousTraitance','acteSousTraitance')
        else:
            self.simple_backup_colonne(self.df,['offresRecues','marcheInnovant','attributionAvance','sousTraitanceDeclaree','dureeMois','variationPrix'])
            self.simple_backup_colonne_inside(self.df,'dureeMois','actesSousTraitance','acteSousTraitance')
            self.simple_backup_colonne_inside(self.df,'variationPrix','actesSousTraitance','acteSousTraitance')

        # Transformation des acheteurs
        if "acheteur" in self.df.columns:
            df_marche = self.df.loc[self.df['nature'].str.contains('March', case=False, na=False)] #on récupère que les lignes de nature "marché"
            self.df.loc[df_marche.index,['id','acheteur']]= self.df.loc[df_marche.index,['id','acheteur']].apply(update_id,axis=1)
        # Force type integer on column offresRecues
        if "dureeMois" in self.df.columns: 
            self.df['dureeMois'] = self.df['dureeMois'].fillna(0).astype(int)
        if "offresRecues" in self.df.columns: 
            self.df['offresRecues'] = self.df['offresRecues'].fillna(0).astype(int)
        if "marcheInnovant" in self.df.columns:
            self.convert_boolean('marcheInnovant')
        if "attributionAvance" in self.df.columns:
            self.convert_boolean('attributionAvance')
        if "sousTraitanceDeclaree" in self.df.columns:
            self.convert_boolean('sousTraitanceDeclaree')
        
        if "dureeMois" in self.df.columns:
            self.df['dureeMois'] = self.df['dureeMois'].astype(int)

        # Arrondi des montants
        if "montant" in self.df.columns:
            self.df['backup__montant'] = self.df['montant']
            self.df['montant'] = self.df['montant'].apply(lambda x: int(x) if pd.notna(x) else np.nan)

        ## Suppression des doublons

        # Conversion des valeurs pour uniformiser les comparaisons
        df_str = self.df.astype(str)
        
        # For statistics purpose only
        excluded_columns = ['report__file','report__error','report__position']
        df_marche = df_str[df_str['_type'].str.contains("Marché")]
        if len(df_marche[df_marche.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])>0:
            self.report.add('Fix/Marchés',self.report.D_DUPLICATE,'Doublon stricts dans la source',df_marche[df_marche.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])
            self.report.nb_duplicated_marches += len(df_marche[df_marche.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])
    
        df_concession = df_str[~df_str['_type'].str.contains("Marché")]
        if len(df_concession[df_concession.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])>0:
            self.report.add('Fix/Concessions',self.report.D_DUPLICATE,'Doublon stricts dans la source',df_concession[df_concession.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])
            self.report.nb_duplicated_concessions += len(df_concession[df_concession.duplicated(subset=df_marche.columns.difference(excluded_columns), keep="last")])

        index_to_keep = df_str.drop_duplicates(subset=df_marche.columns.difference(['report__file','report__nbtotal','report__error','report__position','tmp__annee_mois']), keep="last").index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)

        if "datePublicationDonnees" in self.df.columns:
            self.df['backup__datePublicationDonnees'] = self.df['datePublicationDonnees']
        else:
            self.df['backup__datePublicationDonnees'] = pd.NA
                
        logging.info(f"Fix de {self.source} OK")
        logging.info(f"Nombre de marchés et de concession dans {self.source} après fix : {len(self.df)}")

    def simple_backup_colonne(self,df: pd.DataFrame,liste_colonnes:list):
        for nom_colonne in liste_colonnes:
            if nom_colonne in df.columns:
                df['backup__' + nom_colonne] = df[nom_colonne]
        return df
    

    def enlever_nc_colonne(self,df: pd.DataFrame,nom_colonne:str) -> pd.DataFrame:
        if nom_colonne in df.columns:
            df['backup__' + nom_colonne] = df[nom_colonne]
            #probleme de reimport si ajout de colonne df[nom_colonne+'_source'] = df[nom_colonne]
            #df[nom_colonne] = df[nom_colonne].replace("NC",np.nan)
            with pd.option_context("future.no_silent_downcasting", True):
                df[nom_colonne] = df[nom_colonne].replace("NC",np.nan).infer_objects(copy=False)
        
        return df

    def simple_backup_colonne_inside(self,df: pd.DataFrame,nom_colonne:str,nom_noeud:str,nom_element:str) -> pd.DataFrame:
        def backup_colonne (content,noeud:str,sous_element:str,colonne:str):
            if isinstance(content,list):
                for element in content:
                    if sous_element in element and isinstance(element[sous_element],dict):
                        element[sous_element]['backup__'+colonne] = element[sous_element][colonne]
            return content
        if nom_noeud in df.columns:
            #probleme de reimport si ajout de colonne df[nom_colonne+'_source'] = df[nom_colonne]
            df[nom_noeud] = df[nom_noeud].apply(backup_colonne,noeud=nom_noeud,sous_element=nom_element,colonne=nom_colonne)
        
        return df
    
    def enlever_nc_colonne_inside(self,df: pd.DataFrame,nom_colonne:str,nom_noeud:str,nom_element:str) -> pd.DataFrame:
        def replace_nc (content,noeud:str,sous_element:str,colonne:str):
            if isinstance(content,list):
                for element in content:
                    if sous_element in element and isinstance(element[sous_element],dict) \
                        and colonne in element[sous_element] and element[sous_element][colonne] == "NC":
                            element[sous_element]['backup__'+colonne] = element[sous_element][colonne]
                            element[sous_element][colonne] = None
            return content
        if nom_noeud in df.columns:
            #probleme de reimport si ajout de colonne df[nom_colonne+'_source'] = df[nom_colonne]
            df[nom_noeud] = df[nom_noeud].apply(replace_nc,noeud=nom_noeud,sous_element=nom_element,colonne=nom_colonne)
        
        return df


    def get_nb_enregistrements(self,dico:dict) -> tuple[int,int]:
        """
        Renvoie le nombre total de marchés et le nombre total de concession contenus dans le dictionnaire

        """
        nb_total_marches = 0
        nb_total_concessions = 0
        if 'marche' in dico and isinstance(dico['marche'],list):
            nb_total_marches += len(dico['marche'])
        elif 'marche' in dico:
            nb_total_marches += 1
        if 'contrat-concession' in dico and isinstance(dico['contrat-concession'],list):
            nb_total_concessions += len(dico['contrat-concession'])
        elif 'contrat-concession' in dico:
            nb_total_concessions += 1
        return nb_total_marches,nb_total_concessions
    
    def fix_statistics(self):
        self.report.fix_statistics(self.source)
