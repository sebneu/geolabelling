
def mapping():
    return {
        "geonames": {
            "properties": {
                "url": {"type": "keyword"},
                "name": {"type": "text"},
                "alternateName": {"type": "text"},
                "parentFeature": {"type": "keyword"},
                "parentFeatureName": {"type": "text"},
                "country": {"type": "keyword"},
                "countryName": {"type": "text"},
                "datasets": {"type": "long"}
            }
        }
    }
