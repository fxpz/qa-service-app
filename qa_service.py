import yaml
import argparse
import requests

import tornado.ioloop
import tornado.web


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, settings, *args, **kwargs):
        super(BaseHandler, self).initialize(*args, **kwargs)
        self._settings = settings


class GetQaPrHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_SEARCH_URL'],
                         params=self._settings['GITHUB_SEARCH_PARAMS'],
                         headers=auth_header)
        jres = r.json()
        pulls = [x['number'] for x in jres['items']]
        branches = []
        for pull in pulls:
            r = requests.get(self._settings['GITHUB_PULLS_URL'] % pull,
                             headers=auth_header)
            branches.append(r.json()['head']['ref'])
        self.write(','.join(branches))


def make_app(settings):
    return tornado.web.Application([
        (r"/get_qa_pr", GetQaPrHandler, dict(settings=settings)),
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
