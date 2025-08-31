import googlesearch
import requests
from bs4 import BeautifulSoup
import json
import webbrowser as wb



def buscar_persona(nombre, busqueda_intesiva=False ,abrir_navegador=None):
    """
        Esta función busca información sobre una persona utilizando GoogleSearch y analiza el contenido de las URLs encontradas.
        Parámetros:
            nombre (str): Nombre de la persona a buscar
            busqueda_intensiva (bool, opcional): Realizar una búsqueda intensiva en las URLs encontradas. Por defecto es False.
            abrir_navegador (bool, opcional): Abrir el navegador con las URLs encontradas. Por defecto es None.
        Retorno:
            None. La función imprime los resultados de la búsqueda intensiva y guarda los títulos obtenidos en un archivo JSON.
        Ejemplo de uso:
            buscar_persona("Juan Perez", busqueda_redes=True, busqueda_intesiva=True, abrir_navegador=True)
    """

    if busqueda_intesiva == True:
        # Realizar búsqueda en Google
        query = nombre
        resultado_intensiva = [url for url in googlesearch.search(query, num=5, stop=5, pause=2)]

        data = {
            "titles": [],
            "metas": [],
            "links": [],
            "paragraphs": [],
            "images": [],
            "headers": [],
            "divs": []
        }
        redes_sociales = ['facebook.com', 
                        'x.com', 
                        'instagram.com', 
                        'linkedin.com' ,
                        'telegram.com', 
                        'pinterest.com',
                        ]

        # Obtener contenido de cada URL y analizar con BeautifulSoup
        for url in resultado_intensiva:
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()  # Verificar si hubo errores en la respuesta
            except requests.RequestException as e:
                print(f"Error al acceder a {url}: {e}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            
            # Recopilar datos de cada página
            data["titles"].extend([title.get_text() for title in soup.find_all("title")])
            data["metas"].extend([meta.attrs for meta in soup.find_all("meta")])
            data["links"].extend([link.get("href") for link in soup.find_all("a")])
            data["paragraphs"].extend([paragraph.get_text() for paragraph in soup.find_all("p")])
            data["images"].extend([image.get("src") for image in soup.find_all("img")])
            data["headers"].extend([header.get_text() for header in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])])
            data["divs"].extend([div.get_text() for div in soup.find_all("div")])

        # Imprimir los resultados de la búsqueda intensiva (solo titles)
        print("\nResultados de la búsqueda intensiva:")
        for title in data["titles"]:
            print(f"Title: {title}")

        for headers in data["headers"]:
            print(f"Headers: {headers}")
            if any(red_social in headers for red_social in redes_sociales):
                print(f"La persona tiene cuenta en: {headers}")
        # Guardar los títulos obtenidos en un archivo JSON
        with open("titles.json", "w", encoding="utf-8") as f:
            json.dump({"titles": data["titles"]}, f, ensure_ascii=False, indent=4)

        # Abre el navegador con las URLs encontradas
        for url in resultado_intensiva[:5]:
            wb.open_new_tab(url)

    
    if __name__== "__name__":
        return None


variable = buscar_persona("Agustin Salas", busqueda_intesiva=True, abrir_navegador=True)


#anexo otra prueba

import asyncio
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import json
import webbrowser as wb
from typing import Dict, List


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """Descarga HTML con manejo de errores y timeout."""
    try:
        async with session.get(url, timeout=ClientTimeout(total=10)) as resp:
            if resp.status == 200 and resp.content_type.startswith("text/html"):
                return await resp.text()
    except Exception as e:
        print(f"[!] Error en {url}: {e}")
    return ""


def parse_html(html: str) -> Dict[str, List[str]]:
    """Extrae datos relevantes de un HTML."""
    data = {"titles": [], "headers": [], "paragraphs": [], "links": []}
    if not html:
        return data

    soup = BeautifulSoup(html, "html.parser")

    # Extraer elementos clave
    title = soup.find("title")
    if title:
        data["titles"].append(title.get_text(strip=True))

    data["headers"].extend(h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3"]))
    data["paragraphs"].extend(p.get_text(strip=True) for p in soup.find_all("p"))
    data["links"].extend(a.get("href") for a in soup.find_all("a") if a.get("href"))

    return data


async def buscar_persona(nombre: str, busqueda_intensiva: bool = False, abrir_navegador: bool = False):
    """
    Busca información sobre una persona en Google y analiza resultados.
    Guarda titles en titles.json y abre navegador si se indica.
    """
    # IMPORTANTE: esto usa googlesearch (no API oficial)
    from googlesearch import search  

    query = nombre
    urls = [url for url in search(query, num=5, stop=5, pause=2)]

    if not busqueda_intensiva:
        print("\nURLs encontradas:")
        for u in urls:
            print(" -", u)
        return

    results: Dict[str, List[str]] = {"titles": [], "headers": [], "paragraphs": [], "links": []}

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [fetch_html(session, url) for url in urls]
        pages = await asyncio.gather(*tasks)

        for html in pages:
            parsed = parse_html(html)
            for k, v in parsed.items():
                results[k].extend(v)

    # Mostrar resultados (titles + headers principales)
    print("\nResultados de la búsqueda intensiva:")
    for t in results["titles"]:
        print("Título:", t)
    for h in results["headers"]:
        print("Header:", h)

    # Guardar JSON con titles
    with open("titles.json", "w", encoding="utf-8") as f:
        json.dump({"titles": results["titles"]}, f, ensure_ascii=False, indent=4)

    # Abrir navegador solo si se pide
    if abrir_navegador:
        for u in urls:
            wb.open_new_tab(u)


if __name__ == "__main__":
    asyncio.run(buscar_persona("Elon Musk", busqueda_intensiva=True, abrir_navegador=False))