import logging

logging.basicConfig(level=logging.INFO)

def normalize(line, options):
    return [line]

if __name__ == '__main__':
    lines = [
        "铁路工程建筑及铁路运输业"
    ]
    for line in lines:
        logging.info(normalize(line, ""))