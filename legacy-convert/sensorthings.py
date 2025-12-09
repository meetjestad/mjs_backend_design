from dataclasses import dataclass
from enum import Enum
import json
import logging
import re

import requests


###########################################################
# Simple typed datastructures to build a filter expression
#
# Supports a very limited subset of boolCommonExpr from
# https://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/abnf/odata-abnf-construction-rules.txt
###########################################################


class QExpression:
    """ Superclass for different kinds of expressions. """
    pass


class QOperator(Enum):
    Eq = 'eq'
    Ne = 'ne'
    Lt = 'lt'
    Le = 'le'
    Gt = 'gt'
    Ge = 'ge'

    Has = 'has'

    Add = 'add'
    Sub = 'sub'
    Mul = 'mul'
    Div = 'div'
    Mod = 'mod'

    And = 'and'
    Or = 'or'

    def __str__(self):
        return self.value


@dataclass
class QLiteral(QExpression):
    value: str | int | float | None

    def __str__(self):
        # TODO: HACK: Strings with quotes now fail
        return json.dumps(self.value).replace('"', "'")


@dataclass
class QField(QExpression):
    name: str

    def __str__(self):
        return self.name


@dataclass
class QOp(QExpression):
    left: QExpression
    operator: QOperator
    right: QExpression

    def __str__(self):
        return f"{self.left} {self.operator} {self.right}"

# TODO: Function/method calls
# TODO: not


class SensorThings:
    """Talk to OGC SensorThings API"""

    def __init__(self, url):
        logging.info(
            "Using Sensorthings API at {}".format(url)
        )

        self.url = url
        self.session = requests.Session()
        self.session.hooks['response'].append(self.log_request)

    def log_request(self, response, *args, **kwargs):
        req = response.request

        extra = ""

        location = response.headers.get('Location', None)
        if location is not None:
            extra = " (Location: %s)" % location

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

    def patch(self, **kwargs):
        return self.request(method="PATCH", **kwargs)

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
        new_path = response.headers["Location"]
        # TODO: Unhack, figure out why this happens, maybe FROST bug or
        # config issue?
        new_path = re.sub('^/v1.1', '', new_path)
        return new_path

    def create_procedure(self, content: str, content_type: str):
        return self.create_object("/procedures", content, content_type)

    def create_system(self, content: str | object, content_type: str):
        return self.create_object("/systems", content, content_type)

    def create_observation(self, multidatastream_id: int, content: str | object):
        path = f"/MultiDatastreams({multidatastream_id})/Observations"
        return self.create_object(path, content, content_type="application/json")

    def create_thing(self, thing):
        return self.create_object("/Things", content=thing, content_type="application/json")

    def delete_object(self, path: str):
        self.delete(path=path)

    def patch_object(self, path: str, content: object, content_type: str):
        self.patch(path=path, json=content, content_type=content_type)

    def patch_thing(self, id: str, content: object):
        self.patch_object(path=f"/Things({id})", content=content, content_type="application/json")

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

    def get_objects_filtered(self, path: str, filter: QExpression, expand: str = ""):
        response = self.get(path=path, params={'$filter': str(filter), '$expand': expand})

        return response.json()['value']
