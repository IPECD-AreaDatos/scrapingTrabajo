import requests
from bs4 import BeautifulSoup
import os
import urllib.parse

def extract_ron_file(base_url, xpath_info):
    """
    Extracts the latest RON file from the given URL.
    """
    print(f"Fetching page: {base_url}")
    response = requests.get(base_url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # The user provided XPath: /html/body/main/div[2]/div/section/div/div/article/div[7]/div/div/p[1]
    # We'll try to find the relevant container. 
    # Based on browser logs, it contains tags for MAR, FEB, ENE.
    
    # Let's search for the first <a> tag inside a paragraph that looks like the right one.
    # We can try to match the structure or just look for the links if they are unique enough.
    # However, to be more robust and follow the "first link" instruction:
    
    # We'll try to find the p tag. Since we don't have XPath, 
    # we'll look for a p tag that contains the month links or look for the section.
    
    # Alternative: Use lxml if possible for direct XPath support.
    try:
        from lxml import etree
        tree = etree.HTML(response.content)
        # XPath provided by user
        xpath = "/html/body/main/div[2]/div/section/div/div/article/div[7]/div/div/p[1]"
        paragraphs = tree.xpath(xpath)
        
        if not paragraphs:
            # Fallback: maybe the index changed? Let's try to be a bit more flexible
            # but still targeting the same area.
            print("XPath not found precisely, trying flexible search...")
            paragraphs = tree.xpath("//article//p[contains(., 'MAR') and contains(., 'ENE')]")

        if paragraphs:
            target_p = paragraphs[0]
            links = target_p.xpath(".//a/@href")
            print(f"Found hrefs: {links}")
            if links:
                # The user wants the FIRST one
                latest_link = links[0]
                # Clean up if it starts with blank:#
                if latest_link.startswith('blank:#'):
                    latest_link = latest_link.replace('blank:#', '', 1)
                
                full_url = urllib.parse.urljoin(base_url, latest_link)
                return full_url
    except ImportError:
        print("lxml not found, falling back to BeautifulSoup traversal...")
        # Finding the paragraph by content or structure
        for p in soup.find_all('p'):
            a_tags = p.find_all('a')
            if a_tags and any(month in p.get_text() for month in ['ENE', 'FEB', 'MAR']):
                latest_link = a_tags[0]['href']
                print(f"Found href via BS4: {latest_link}")
                if latest_link.startswith('blank:#'):
                    latest_link = latest_link.replace('blank:#', '', 1)
                full_url = urllib.parse.urljoin(base_url, latest_link)
                return full_url

    return None

def download_file(url, target_path):
    print(f"Downloading: {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(target_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved to: {target_path}")

if __name__ == "__main__":
    URL = "https://www.argentina.gob.ar/economia/sechacienda/asuntosprovinciales/ron"
    # Create the directory if it doesn't exist
    os.makedirs("files/raw", exist_ok=True)
    
    file_url = extract_ron_file(URL, None)
    if file_url:
        target = os.path.join("files", "raw", "ron_raw.xls")
        download_file(file_url, target)
    else:
        print("Could not find the download link.")
