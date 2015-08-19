# By: Carlos Eduardo Moreira dos Santos, 2014.
# Based on: Kelcey Damage, 2012 & Kraig Amador, 2012
# Changes:
#   - ported to python3
#   - support for config.ini (url, key, secret)
#   - method calls work without any parameter
#   - pep8 compliance

from expyrimenter.core import Config, ExpyLogger
from urllib.parse import quote_plus
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
import base64
import hashlib
import hmac
import json


class SignedAPICall():
    def __init__(self, api_url, api_key, api_secret):
        self.url = api_url
        self.key = api_key
        self.secret = api_secret

    def request(self, args):
        args['apiKey'] = self.key

        self.params = []
        self._sort_request(args)
        self._create_signature()
        self._build_get_request()

    def _sort_request(self, args):
        keys = sorted(args.keys())

        for key in keys:
            self.params.append(key + '=' + self._quote(args[key]))

    def _quote(self, value):
        if value is True:
            quoted = 'true'
        elif value is False:
            quoted = 'false'
        else:
            quoted = quote_plus(value)

        return quoted

    def _create_signature(self):
        self.query = '&'.join(self.params)
        digest = hmac.new(self.secret.encode(),
                          msg=self.query.lower().encode(),
                          digestmod=hashlib.sha1).digest()

        self.signature = base64.b64encode(digest)

    def _build_get_request(self):
        self.query += '&signature=' + quote_plus(self.signature)
        self.value = self.url + '?' + self.query


class API(SignedAPICall):
    def __init__(self):
        cfg = Config('cloudstack')
        super().__init__(cfg.get('url'), cfg.get('key'), cfg.get('secret'))
        self._logger = ExpyLogger.getLogger('cloudstack.api')

    def __getattr__(self, name):
        def handlerFunction(*args, **kwargs):
            if kwargs:
                return self._make_request(name, kwargs)
            if len(args) > 0:
                raise TypeError('API call parameters must be named:\n'
                                '           '
                                "api.command(param1='value1', param2='value2'"
                                ', ...)')
            return self._make_request(name, {})

        return handlerFunction

    def _http_get(self, url):
        self._logger.debug(url)
        try:
            response = urlopen(url)
        except (HTTPError, URLError) as e:
            msg = 'URL was "%s"'
            args = [url]
            self._logger.failure(title='HTTP Get', exception=e, extra_msg=msg,
                                 extra_args=args)
            raise e

        return response.read()

    def _make_request(self, command, args):
        args['response'] = 'json'
        args['command'] = command
        self.request(args)
        data = self._http_get(self.value).decode()
        # The response is of the format {commandresponse: actual-data}
        key = command.lower() + "response"
        return json.loads(data)[key]
