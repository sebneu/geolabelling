
from geopy.geocoders import Nominatim
import rdflib
from rdflib import Namespace, URIRef
WGS84 = Namespace('http://www.w3.org/2003/01/geo/wgs84_pos#')


class NominatimService:
    def __init__(self):
        self.geolocator = Nominatim()

    def streets_for_geoname(self, geonames_id):
        g = rdflib.Graph()
        g_id = g.parse(geonames_id + 'about.rdf')
        lat = g_id.value(subject=URIRef(geonames_id), predicate=WGS84.lat)
        long = g_id.value(subject=URIRef(geonames_id), predicate=WGS84.long)

        location = self.geolocator.reverse((lat, long))
        print location.address
        print location.latitude, location.longitude
        print location.raw
        print

    def address_for_string(self, name):
        location = self.geolocator.geocode(name)
        print location.address
        print location.latitude, location.longitude
        print
