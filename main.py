#encoding: utf-8
# 
# Copyright (c) 2016, Internet Initiative Japan, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""神楽坂サイエンスアカデミー2016用に作成。

influxdb の API を変換・制限するためのサーバ。
"""

import base64
import logging

import tornado.gen
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web


Users = dict(
    # ユーザー名="パスワード", のように定義
    team_Z="password",
)


class InfluxDBHandler(tornado.web.RequestHandler):
    Labels = ("mA", "mV", "A", "V", "W", "mW", "m/s")
    URL = "http://localhost:8086/write?db=ksa2016"

    def __init__(self, *args, **kwargs):
        self.username = None
        super(InfluxDBHandler, self).__init__(*args, **kwargs)

    @tornado.gen.coroutine
    def prepare(self):
        """Basic 認証する。

        認証が成功した場合は、post() 内で self.usrename を参照できる。
        """
        credentials = self.request.headers.get('Authorization')
        if not credentials:
            logging.info("no Authorization header")
            raise tornado.web.HTTPError(401, reason='Unauthorized')
        a = credentials.split()
        if len(a) == 2 and a[0].lower() == 'basic':
            credential = a[1]
        else:
            logging.info('require Basic Authentication')
            raise tornado.web.HTTPError(401, reason='Unauthorized')

        try:
            auth_decoded = base64.b64decode(credential).decode('utf-8')
            username, user_password = auth_decoded.split(':', 1)
        except (TypeError, ValueError):
            logging.info("invalid Authorization Header")
            raise tornado.web.HTTPError(401, reason='Unauthorized')

        if Users.get(username, -1) == user_password:
            self.username = username
        else:
            raise tornado.web.HTTPError(401, reason='Unauthorized')

    @tornado.gen.coroutine
    def post(self):
        assert self.username is not None
        team = self.username
        logging.info("team=%s request=%s", team, self.request.body)

        user_request_body = self.request.body.decode("ascii")
        influx_request = ""
        for line in user_request_body.splitlines():
            try:
                label, value = line.split(" ", 1)
                value = value.strip()
            except ValueError:  # unpack error
                raise tornado.web.HTTPError(400, "invalid request")
            if label not in self.Labels:
                logging.info("unknown label: %s", label)
                continue
            try:
                fvalue = float(value)
            except ValueError:  # convert error
                logging.info("invalid value: %s (line=%s)", value, line)
                continue
            influx_request += "%s,team=%s value=%f\n" % (label, team, fvalue)
        if influx_request == "":
            raise tornado.web.HTTPError(400, "empty request")

        logging.debug("team=%s request=%s", team, influx_request)

        client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(self.URL, method="POST", body=influx_request)
        response = yield client.fetch(request, raise_error=False)
        logging.info("team=%s code=%s reason=%s", team, response.code, response.reason)


def main():
    tornado.options.parse_command_line()

    handlers = [
        (r"/db/?", InfluxDBHandler),
    ]
    app = tornado.web.Application(handlers)
    server = tornado.httpserver.HTTPServer(app)
    server.listen(8801, address="127.0.0.1")

    logging.info("starting influx-adapter")
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
