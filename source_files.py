import requests
from bs4 import BeautifulSoup
from pathlib import Path, PurePath
from datetime import datetime
import logging

'''Los archivos fuentes serán utilizados para popular la base de datos'''



def files():
    url ={"Museos": "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_4207def0-2ff7-41d5-9095-d42ae8207a5d",
      "Cines": "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_392ce1a8-ef11-4776-b280-6f1c7fae16ae",
      "Bibliotecas": "https://datos.gob.ar/dataset/cultura-mapa-cultural-espacios-culturales/archivo/cultura_01c6c048-dbeb-44e0-8efa-6944f73715d7"
      }
    
    for key, value in url.items():
        try:
                
            r = requests.get(value)
            soup = BeautifulSoup(r.content, "html.parser")
            download =soup.find("a", class_="btn btn-green btn-block")
            fil = download.get("href")
            page = requests.get(fil)
            status = page.status_code
            if status == 200:
                now = datetime.now()
                root = Path.cwd()
                folder = PurePath.joinpath(root, key, now.strftime('%Y-%B'))
                folder.mkdir(parents=True, exist_ok=True)
                file_csv = (f"{key}-{now.strftime('%d-%m-%Y')}.csv")
                with open(f"{folder}/{file_csv}", "wb") as f:
                    f.write(page.content)
                    logging.info(f'Descarga correcta de {file_csv}')
        except Exception:
            logging.error(f'Fallo en la descarga{file_csv}')

if __name__ == "__main__":  
    files()