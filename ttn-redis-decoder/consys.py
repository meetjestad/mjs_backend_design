import db
import os
import logging
import urllib

import requests
from pony import orm


class ConnectedSystems:
    """Talk to OGC Connected Systems API"""

    def __init__(self, url):
        logging.info(
            "Using Consys API at {}".format(url)
        )

        self.url = url
        self.urn_root = 'urn:fdc:meetjestad.nl:2024'
        self.session = requests.Session()
        self.session.hooks['response'].append(self.log_request)

    def log_request(self, response, *args, **kwargs):
        req = response.request

        extra = ""

        location = response.headers.get('Location', None)
        if location is not None:
            extra = "(Location: %s)" % location

        logging.debug("%s %s -> %s%s", req.method, req.url, response.status_code, extra)
        if req.method == 'POST' and req.body:
            logging.debug("POST data: %s", req.body)
        if response.status_code == 200 and response.content:
            logging.debug("Response data: %s", response.content)

    def post(self, url=None, path=None, content_type=None, **kwargs):
        assert (url and not path) or (path and not url)

        if not url:
            url = self.url + path

        kwargs['headers'] = kwargs.pop('headers', {})
        if content_type:
            kwargs['headers']['Content-Type'] = content_type

        response = self.session.post(url, **kwargs)
        response.raise_for_status()
        return response

    def process_config_message(self, obj: db.Config):
        system_urn = "{}:system:{}".format(self.urn_root, obj.node_id)

        system = {
            "type": "PhysicalSystem",
            # "id": "abcd", # Ignored by OSH?
            "definition": "http://www.w3.org/ns/sosa/Sensor",
            "uniqueId": system_urn,
            "label": obj.node_id,
            "description": "TODO",
            # "typeOf": {
            #     "href": "https://data.example.org/api/procedures/TP60S?f=sml",
            #     "uid": "urn:x-myorg:datasheets:ThermoPro:TP60S:v001",
            #     "title": "ThermoPro TP60S",
            #     "type": "application/sml+json",
            # },
        }

        system_response = self.post(path="/systems", json=system, content_type="application/sml+json")
        assert system_response.status_code == 201
        # This should redirect to the id/path of the added system
        system_path = system_response.headers["Location"]

        fields = []
        field_names = {}
        for chan_id, channel in obj.data["channel_config"].items():
            quantity_url = channel["quantity"]
            # TODO: Explicitly specify name in node?
            name = os.path.basename(urllib.parse.urlparse(quantity_url).path)
            fields.append({
                "type": "Quantity",
                "name": name,
                "definition": quantity_url,
                "label": "TODO",
                "description": "TODO",
                # TODO: Units can also be a href
                "uom": {
                    "code": channel["unit"],
                },
            })
            field_names[chan_id] = name

        datastream = {
            "name": obj.node_id,
            "description": "TODO",
            # "ultimateFeatureOfInterest@link": {
            #     "href": "https://data.example.org/api/collections/buildings/items/754",
            #     "title": "My House"
            # },
            # "samplingFeature@link": {
            #     "href": "https://data.example.org/api/samplingFeatures/4478",
            #     "title": "Thermometer Sampling Point"
            # },
            "outputName": "data",
            "schema": {
                "obsFormat": "application/om+json",
                "resultTimeSchema": {
                    "name": "time",
                    "type": "Time",
                    "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                    "referenceFrame": "http://www.opengis.net/def/trs/BIPM/0/UTC",
                    "uom": {
                        "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                    }
                },
                "resultSchema": {
                    "type": "DataRecord",
                    "fields": fields,
                }
            }
        }

        datastream_response = self.post(
            path=system_path + "/datastreams", json=datastream, content_type="application/json"
        )
        # This should redirect to the id/path of the added datastream
        assert datastream_response.status_code == 201
        datastream_path = datastream_response.headers["Location"]

        prefix = "/datastreams/"
        assert datastream_path.startswith(prefix)
        datastream_id = datastream_path[len(prefix):]

        obj.datastream_id = datastream_id
        obj.field_names = field_names
        # TODO: This is out of place, might commit other stuff, etc.
        orm.commit()

    def process_data_message(self, obj: db.Bundle):
        observation = {
            "resultTime": obj.timestamp.isoformat(),
            "phenomenonTime": obj.timestamp.isoformat(),
            "result": {},
        }

        for channel in obj.data.values():
            name = obj.config.field_names[str(channel['channel_id'])]
            observation['result'][name] = channel["value"]
        # TODO: Check if all fields are present? OSH rejects the
        # observation otherwise

        observations_path = "/datastreams/{}/observations/".format(obj.config.datastream_id)
        observation_response = self.post(
            path=observations_path, json=observation, content_type="application/om+json"
        )

        # This should redirect to the id/path of the added datastream
        assert observation_response.status_code == 201
