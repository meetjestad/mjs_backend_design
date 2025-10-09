import logging

import requests


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
        if response.content:
            logging.debug("Response data: %s", response.content)

        if not response:
            logging.warning(f"{req.method} {req.url} failed with {response.status_code}\n{response.content.decode()}")

    def request(self, method, url=None, path=None, content_type=None, **kwargs):
        assert (url and not path) or (path and not url)

        if not url:
            url = self.url + path

        kwargs['headers'] = kwargs.pop('headers', {})
        if content_type:
            kwargs['headers']['Content-Type'] = content_type

        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get(self, **kwargs):
        return self.request(method="GET", **kwargs)

    def post(self, **kwargs):
        return self.request(method="POST", **kwargs)

    def delete(self, **kwargs):
        return self.request(method="DELETE", **kwargs)

    def create_object(self, path: str, content: str | object, content_type: str):
        if isinstance(content, str):
            data = content
            json = None
        else:
            data = None
            json = content
        response = self.post(path=path, data=data, json=json, content_type=content_type)
        assert response.status_code == 201
        # This should redirect to the id/path of the added object
        return response.headers["Location"]

    def create_procedure(self, content: str, content_type: str):
        return self.create_object("/procedures", content, content_type)

    def create_system(self, content: str | object, content_type: str):
        return self.create_object("/systems", content, content_type)

    def create_datastream(self, system_path: str, content: str | object, content_type: str):
        return self.create_object(system_path + "/datastreams", content, content_type)

    def create_observation(self, datastream_path: str, content: str | object, content_type: str):
        return self.create_object(datastream_path + "/observations", content, content_type)

    def delete_object(self, path: str):
        self.delete(path=path)

    def get_object_by_id(self, path: str, id: str):
        # id filter should also filter by uid according to consys spec,
        # but osh uses different param for that. See:
        # sensorhub-service-consys/src/main/java/org/sensorhub/impl/service/consys/resource/ResourceHandler.java
        # sensorhub-service-consys/src/main/java/org/sensorhub/impl/service/consys/feature/AbstractFeatureHandler.java
        # Fixed 2025-03-26
        response = self.get(path=path, params={'id': id})
        # TODO generalize with by_uid below?
        return response

    def get_object_by_uid(self, path: str, uid: str):
        response = self.get(path=path, params={'uid': uid})

        objs = response.json()['items']
        if not objs:
            return None
        assert len(objs) == 1
        return objs[0]
