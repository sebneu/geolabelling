
def mapping():
    return {
        "geonames": {
            "properties": {
                "id": {"type": "keyword"},
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
