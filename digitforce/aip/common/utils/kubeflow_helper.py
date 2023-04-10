import re
import requests
from urllib.parse import urlsplit
import digitforce.aip.common.utils.config_helper as config_helper
import digitforce.aip.common.utils.id_helper as id_helper
from kfp.compiler import Compiler
import kfp
import datetime


def get_istio_auth_session(url: str, username: str, password: str) -> dict:
    """
    Determine if the specified URL is secured by Dex and try to obtain a session cookie.
    参考：https://www.kubeflow.org/docs/components/pipelines/v1/sdk/connect-api/#full-kubeflow-subfrom-outside-clustersub
    WARNING: only Dex `staticPasswords` and `LDAP` authentication are currently supported
             (we default default to using `staticPasswords` if both are enabled)

    :param url: Kubeflow server URL, including protocol
    :param username: Dex `staticPasswords` or `LDAP` username
    :param password: Dex `staticPasswords` or `LDAP` password
    :return: auth session information
    """
    # define the default return object
    auth_session = {
        "endpoint_url": url,  # KF endpoint URL
        "redirect_url": None,  # KF redirect URL, if applicable
        "dex_login_url": None,  # Dex login URL (for POST of credentials)
        "is_secured": None,  # True if KF endpoint is secured
        "session_cookie": None  # Resulting session cookies in the form "key1=value1; key2=value2"
    }

    # use a persistent session (for cookies)
    with requests.Session() as s:

        ################
        # Determine if Endpoint is Secured
        ################
        resp = s.get(url, allow_redirects=True)
        if resp.status_code != 200:
            raise RuntimeError(
                f"HTTP status code '{resp.status_code}' for GET against: {url}"
            )

        auth_session["redirect_url"] = resp.url

        # if we were NOT redirected, then the endpoint is UNSECURED
        if len(resp.history) == 0:
            auth_session["is_secured"] = False
            return auth_session
        else:
            auth_session["is_secured"] = True

        ################
        # Get Dex Login URL
        ################
        redirect_url_obj = urlsplit(auth_session["redirect_url"])

        # if we are at `/auth?=xxxx` path, we need to select an auth type
        if re.search(r"/auth$", redirect_url_obj.path):
            #######
            # TIP: choose the default auth type by including ONE of the following
            #######

            # OPTION 1: set "staticPasswords" as default auth type
            redirect_url_obj = redirect_url_obj._replace(
                path=re.sub(r"/auth$", "/auth/local", redirect_url_obj.path)
            )
            # OPTION 2: set "ldap" as default auth type
            # redirect_url_obj = redirect_url_obj._replace(
            #     path=re.sub(r"/auth$", "/auth/ldap", redirect_url_obj.path)
            # )

        # if we are at `/auth/xxxx/login` path, then no further action is needed (we can use it for login POST)
        if re.search(r"/auth/.*/login$", redirect_url_obj.path):
            auth_session["dex_login_url"] = redirect_url_obj.geturl()

        # else, we need to be redirected to the actual login page
        else:
            # this GET should redirect us to the `/auth/xxxx/login` path
            resp = s.get(redirect_url_obj.geturl(), allow_redirects=True)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HTTP status code '{resp.status_code}' for GET against: {redirect_url_obj.geturl()}"
                )

            # set the login url
            auth_session["dex_login_url"] = resp.url

        ################
        # Attempt Dex Login
        ################
        resp = s.post(
            auth_session["dex_login_url"],
            data={"login": username, "password": password},
            allow_redirects=True
        )
        if len(resp.history) == 0:
            raise RuntimeError(
                f"Login credentials were probably invalid - "
                f"No redirect after POST to: {auth_session['dex_login_url']}"
            )

        # store the session cookies in a "key1=value1; key2=value2" string
        auth_session["session_cookie"] = "; ".join([f"{c.name}={c.value}" for c in s.cookies])

    return auth_session


def upload_pipeline(pipeline_func, pipeline_name):
    kubeflow_config = config_helper.get_module_config("kubeflow")
    pipeline_path = f"/tmp/{pipeline_name}.yaml"
    pipeline_conf = kfp.dsl.PipelineConf()
    pipeline_conf.set_image_pull_policy("Always")
    Compiler().compile(pipeline_func=pipeline_func, package_path=pipeline_path, pipeline_conf=pipeline_conf)
    client = kfp.Client(host=kubeflow_config['url'], cookies=get_istio_auth_session(
        url=kubeflow_config['url'], username=kubeflow_config['username'],
        password=kubeflow_config['password'])['session_cookie'])
    try:
        return client._upload_api.upload_pipeline(uploadfile=pipeline_path, name=pipeline_name)
    except Exception as e:
        print('there maybe a pipeline with the same name, try to update it')
        return upload_pipeline_version(pipeline_func, get_pipeline_id(pipeline_name), pipeline_name)


def upload_pipeline_version(pipeline_func, pipeline_id, pipeline_name):
    kubeflow_config = config_helper.get_module_config("kubeflow")
    pipeline_path = f"/tmp/{pipeline_name}.yaml"
    pipeline_conf = kfp.dsl.PipelineConf()
    pipeline_conf.set_image_pull_policy("Always")
    Compiler().compile(pipeline_func=pipeline_func, package_path=pipeline_path, pipeline_conf=pipeline_conf)
    client = kfp.Client(host=kubeflow_config['url'], cookies=get_istio_auth_session(
        url=kubeflow_config['url'], username=kubeflow_config['username'],
        password=kubeflow_config['password'])['session_cookie'])
    return client._upload_api.upload_pipeline_version(uploadfile=pipeline_path, pipelineid=pipeline_id,
                                                      name=f'{pipeline_name}-{id_helper.gen_uniq_id()}')


def create_run_directly(pipeline_name, pipeline_func, experiment_name, arguments, ):
    kubeflow_config = config_helper.get_module_config("kubeflow")
    client = kfp.Client(host=kubeflow_config['url'], cookies=get_istio_auth_session(
        url=kubeflow_config['url'], username=kubeflow_config['username'],
        password=kubeflow_config['password'])['session_cookie'])
    pipeline_conf = kfp.dsl.PipelineConf()
    pipeline_conf.set_image_pull_policy("Always")
    client.create_run_from_pipeline_func(
        pipeline_func,
        run_name=pipeline_name + ' ' + datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'),
        arguments=arguments,
        pipeline_conf=pipeline_conf,
        experiment_name=experiment_name,
        namespace='kubeflow-user-example-com'
    )


def get_pipeline_id(pipeline_name):
    kubeflow_config = config_helper.get_module_config("kubeflow")
    client = kfp.Client(host=kubeflow_config['url'], cookies=get_istio_auth_session(
        url=kubeflow_config['url'], username=kubeflow_config['username'],
        password=kubeflow_config['password'])['session_cookie'])
    # 参考：https://github.com/kubeflow/pipelines/blob/master/backend/api/v1beta1/filter.proto
    f = {
        "predicates":
            [
                {
                    "key": "name",
                    "op": "EQUALS",
                    "string_value": pipeline_name
                }
            ],
    }
    import json
    pipeline_list = client._pipelines_api.list_pipelines(filter=json.dumps(f)).pipelines
    if pipeline_list is None or len(pipeline_list) == 0:
        return None
    return pipeline_list[0].id
