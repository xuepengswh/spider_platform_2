from urllib.parse import urljoin



def normalize(line, base_url):
    end_url = urljoin(base_url,line)
    return [end_url]
