import yaml
import argparse
import requests

import tornado.ioloop
import tornado.web

from tornado.escape import json_encode


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, settings, *args, **kwargs):
        super(BaseHandler, self).initialize(*args, **kwargs)
        self._settings = settings


class GetWebQaPrHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_PR_SEARCH_URL'],
                         params=self._settings['GITHUB_PR_WEB_SEARCH_PARAMS'],
                         headers=auth_header)
        jres = r.json()
        pulls = [x['number'] for x in jres['items']]
        branches = []
        for pull in pulls:
            r = requests.get(self._settings['GITHUB_PR_WEB_URL'] % pull,
                             headers=auth_header)
            branches.append({"name": r.json()['head']['ref'],
                             "value": r.json()['head']['ref']})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))


class GetApiBranchesHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_BR_API_URL'],
                         headers=auth_header)
        branches = [{'name': x['name'], 'value': x['name']} for x in r.json()]
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))


class GetQaServersHandler(BaseHandler):
    def get(self):
        import re
        import pyrax

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(
            self._settings['RAX_CREDS_FILE'],
            region=self._settings['RAX_REGION'])
        cs = pyrax.cloudservers
        server_list = cs.servers.list()
        servers = []
        r = re.compile(r'qa-(\d+)$')
        for i in server_list:
            if r.match(i.name):
                servers.append({
                    'name': i.name,
                    'value': r.match(i.name).group(1)})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(servers))


def make_app(settings):
    return tornado.web.Application([
        (r"/web/get_qa_pr", GetWebQaPrHandler, dict(settings=settings)),
        (r"/api/get_branches", GetApiBranchesHandler, dict(settings=settings)),
        (r"/get_qa_servers", GetQaServersHandler, dict(settings=settings))
    ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-p', '--port', help='listen port',
                        default=8888, type=int)
    parser.add_argument('-c', '--conf', help='config file path',
                        default='./qa_service.yml')
    args = parser.parse_args()
    with open(args.conf) as f:
        settings = yaml.load(f)

    app = make_app(settings)
    app.listen(settings['port'])
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
