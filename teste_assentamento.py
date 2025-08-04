import urllib3
from urllib3.util.ssl_ import create_urllib3_context
import ssl
import requests
import os
import zipfile
import shapely
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
import tqdm
import warnings

engine = create_engine('postgresql://postgres:postgres@localhost:5432/ETL')

# ignorar avisos desnecessarios
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Classe CustomHttpAdapter para configurar o SSL
class CustomHttpAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.set_ciphers('DEFAULT@SECLEVEL=1')
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)
    
# Função para obter URLs dos arquivos
def get_urls():
    return [
        {  # Assentamentos SAB INCRA
            "url": "https://www.gov.br/insa/pt-br/centrais-de-conteudo/mapas/mapas-em-shapefile/assentamentos-sab-incra.zip/@@download/file",
            "filename": "Assentamentos_SAB_INCRA.zip",
        },
        {  # Terras Indígenas Poligonais
            "url": "https://geoserver.funai.gov.br/geoserver/Funai/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=Funai%3Atis_poligonais&maxFeatures=10000&outputFormat=SHAPE-ZIP",
            "filename": "tis_poligonais.zip",
        },
        {  # Municipios IBGE
            "url": "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/Brasil/BR_Municipios_2024.zip",
            "filename": "BR_Municipios_2024.zip",
        }
    ]

# Função para baixar arquivos
def download_files():
    urls = get_urls()
    for url_info in urls:
        filename = os.path.join(os.path.dirname(__file__), url_info["filename"])
        if os.path.exists(filename):
            print(f"File {filename} already exists, skipping download.")
            continue
        url = url_info["url"]
        
        with requests.get(url, stream=True, verify=False) as response:
            response.raise_for_status()
            total = int(response.headers.get('content-length', 0))
            with open(filename, 'wb') as file, tqdm.tqdm(
                desc=f"Downloading {url_info['filename']}",
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    bar.update(len(chunk))
        print(f"Downloaded {filename}")

# Função para extrair arquivos ZIP
def extract_zip(filename, target_folder=None):
    if not zipfile.is_zipfile(filename):
        print(f"{filename} não é um arquivo ZIP válido")
        return
        
    if target_folder is None:
        target_folder = os.path.join(os.path.dirname(filename), os.path.splitext(os.path.basename(filename))[0])

    os.makedirs(target_folder, exist_ok=True)

    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(target_folder)
    print(f"Extraído: {filename} para {target_folder}")
        
def main():
    download_files()
    for url_info in get_urls():
        filename = url_info["filename"]
        extract_zip(filename)
    
    # # Monta os caminhos dos shapefiles extraídos
    # files = {
    #     "Terras_Indigenas": os.path.join("tis_poligonais", "tis_poligonaisPolygon.shp"),
    #     "Assentamentos_INCRA": os.path.join("Assentamentos_SAB_INCRA/Assentamentos-SAB-INCRA", "Assentamentos-SAB-INCRA.shp"),
    #     "Municipios": os.path.join("BR_Municipios_2024", "BR_Municipios_2024.shp")
    # }
    
    # for key, file_path in files.items():
    #     warnings.filterwarnings("ignore", category=RuntimeWarning)
    #     path = gpd.read_file(file_path)
     
    #     if path.empty:
    #         print(f"Arquivo {file_path} está vazio ou não foi encontrado.")
    #         continue
        
    #     # Verifica se a geometria é MultiPolygon e converte se necessário
    #     if not path.geometry.apply(lambda geom: geom.geom_type == "MultiPolygon").all():
    #         path["geometry"] = path["geometry"].apply(
    #             lambda geom: geom if geom.geom_type == "MultiPolygon" else shapely.geometry.MultiPolygon([geom])
    #         )
        
    #     # Carrega os dados no PostGIS
    #     path.to_postgis(key, engine, schema="datas", index=False, if_exists='replace')
    #     print(f"Dados {key} carregados no esquema PostGIS 'datas'.")

if __name__ == "__main__":
    main()