try:
    import ujson as json
except:
    import json

from tornado.web import RequestHandler

class BaseApiHandler(RequestHandler):
    _JSON_MIMETYPES = set([
        'application/json',
        'application/json; charset=UTF-8',
        'application/json; charset=utf-8',
        'application/json+protobuf',
        'application/json+protobuf; charset=UTF-8',
        'application/json+protobuf; charset=utf-8',
    ])

    def IS_JSON_MIMETYPES(self, content_mimetype):
        return content_mimetype in BaseApiHandler._JSON_MIMETYPES

    def do_set_base_headers(self):
        # 允许跨域请求
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header("Server", "XGateWay")

    def set_default_headers(self):
        self.do_set_base_headers()

    def finish(self, chunk=None):
        self.set_default_headers()
        super(BaseApiHandler, self).finish(chunk)

    def options(self):
        self.set_status(204)
        self.finish()

    def get_params(self, param_name, ptype=0, default=None):
        '''
        get_params(self, param_name, ptype=0, default=None):
        从请求包里 获取参数

        :param param_name:  要获取的参数名
        :param ptype:       获取的途径 0 从GET请求获取 1 从POST请求获取
        :param default:     参数不存在时返回的默认值
        :return:
        '''
        # 获取get的参数
        if ptype == 0:
            return self.get_query_argument(param_name, default)
        # 获取post的参数
        elif ptype == 1:
            # 如果是正常的post参数
            if not self.IS_JSON_MIMETYPES(self.request.headers.get('Content-Type', "")):
                return self.get_body_argument(param_name, default)
            # 如果post的是json 所有参数都是在计算body的json里
            else:
                if self.request.body:
                    try:
                        json_data = json.loads(self.request.body)
                        return json_data.get(param_name, default)
                    except:
                        pass
                return default
        return default


#---------------------------------------------------------------------------
class JsonHandler(BaseApiHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.do_set_base_headers()

#---------------------------------------------------------------------------
class HtmlHandler(BaseApiHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "text/html; charset=UTF-8")
        self.do_set_base_headers()

if __name__ == "__main__":
    pass