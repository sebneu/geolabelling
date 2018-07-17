
from v1 import mapping, generateFromYACPTable
import geonames

mappings={
    'v1': {
        'mapping': mapping,
        'generator':generateFromYACPTable
    },
    'geonames': {
        'mapping': geonames.mapping()
    }
}