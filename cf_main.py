import google.auth
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account

import requests

from yaml import load, Loader
import datetime
import json
import os
import random
import flask

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

def getGcpServiceAccount(client_id):
    bootstrap_credentials, _ = google.auth.default(
        scopes=[IAM_SCOPE])

    bootstrap_credentials.refresh(Request())

    signer_email = bootstrap_credentials.service_account_email
    if isinstance(bootstrap_credentials,
                  google.auth.compute_engine.credentials.Credentials):
        signer = google.auth.iam.Signer(
            Request(), bootstrap_credentials, signer_email)
    else:
        signer = bootstrap_credentials.signer
    service_account_credentials = google.oauth2.service_account.Credentials(
        signer, signer_email, token_uri=OAUTH_TOKEN_URI, additional_claims={
            'target_audience': client_id
        })
    
    return service_account_credentials, signer_email
    
    
def getOpenIDToken(client_id):
    service_account_credentials, _ = getGcpServiceAccount(client_id)
    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials)
    return google_open_id_connect_token

# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.

    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.

    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    service_account_credentials, signer_email = getGcpServiceAccount(client_id)
    # service_account_credentials gives us a JWT signed by the service
    # account. Next, we use that to obtain an OpenID Connect token,
    # which is a JWT signed by Google.
    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    # print('{!r} / {!r} / {!r} / {!r} / {!r} / {!r}'.format(
    #             resp.status_code, resp.headers, resp.text, resp.request.url, resp.request.headers, resp.request.method))
    if resp.status_code == 403:
        raise Exception('Service account {} does not have permission to '
                        'access the IAP-protected application.'.format(
                            signer_email))
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r} / {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text, resp.request.url, resp.request.headers, resp.request.method))
    else:
        return resp.text


def get_google_open_id_connect_token(service_account_credentials):
    """Get an OpenID Connect token issued by Google for the service account.

    This function:

      1. Generates a JWT signed with the service account's private key
         containing a special "target_audience" claim.

      2. Sends it to the OAUTH_TOKEN_URI endpoint. Because the JWT in #1
         has a target_audience claim, that endpoint will respond with
         an OpenID Connect token for the service account -- in other words,
         a JWT signed by *Google*. The aud claim in this JWT will be
         set to the value from the target_audience claim in #1.

    For more information, see
    https://developers.google.com/identity/protocols/OAuth2ServiceAccount .
    The HTTP/REST example on that page describes the JWT structure and
    demonstrates how to call the token endpoint. (The example on that page
    shows how to get an OAuth2 access token; this code is using a
    modified version of it to get an OpenID Connect token.)
    """

    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion())
    request = google.auth.transport.requests.Request()
    body = {
        'assertion': service_account_jwt,
        'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, OAUTH_TOKEN_URI, body)
    return token_response['id_token']
# END COPIED IAP CODE


class Airflow():

    URL_SUFIX = '/api/experimental'

    def __init__(self, base_url, auth_type, auth=None, iap_client_id=None):

        self._base_url = base_url + self.URL_SUFIX

        if auth_type == 'iap':
            self._iap = True
            self._iap_client_id = iap_client_id
        else:
            raise NotImplementedError('other than IAP authentication methods are not supported')

    @classmethod
    def iap_auth(cls, webserver_id=None, client_id=None):

        if webserver_id is None:
            webserver_id = os.getenv('COMPOSER_WEBSERVER_ID')
        base_url = 'https://' + webserver_id + '.appspot.com'

        if client_id is None:
            client_id = os.getenv('COMPOSER_CLIENT_ID')
        

        return cls(
                    base_url=base_url,
                    auth_type='iap',
                    iap_client_id= client_id
                )

    def trigger_dag(
            self,
            dag_id,
            run_id=None,
            conf=None,
            execution_date=None
    ):
        """Create a dag run for the specified dag.
        :param dag_id:
        :param run_id:
        :param conf:
        :param execution_date:
        :return:
        """

        url = self.url + '/dags/' + dag_id + '/dag_runs'
        method = 'POST'

        payload= {}

        if run_id is not None:
            payload.update(run_id = run_id + '_' + self.get_run_id_sufix(dag_id))
        else:
            payload.update(run_id = self.get_run_id_sufix(dag_id))

        if conf is not None:
            payload.update(conf = conf)

        if execution_date is not None:
            payload.update(execution_date = execution_date)

        return self._make_request(url=url, method=method, data=json.dumps(payload))

    @property
    def url(self):
        return self._base_url

    @staticmethod
    def get_run_id_sufix(dag_id=None):
        """
        This function builds a nice runId, format dag_id_YYYYMMDDHmS_randomInt
        Returns:
        A string with the runId
        """
        myDate = datetime.datetime.now()
        dateIntStr = str(myDate.year) + ("0" + str(myDate.month))[-2:] + ("0" + str(myDate.day))[-2:]
        dateIntStr = dateIntStr + ("0" + str(myDate.hour))[-2:] + ("0" + str(myDate.minute))[-2:]
        dateIntStr = dateIntStr + ("0" + str(myDate.second))[-2:] + "_" + str(random.randint(10, 999))

        if dag_id:
            run_id_sufix = str(dag_id) + "_" + dateIntStr
        else:
            run_id_sufix = 'DEMO_' + dateIntStr

        return run_id_sufix

    def _make_request(self, url, **kwargs):

        if self._iap:
            iap_output = make_iap_request(url=url, client_id=self._iap_client_id, **kwargs)
            response = iap_output
        
            return json.loads(response)

def devfest(request):

    data = request.data.decode('UTF-8')
    data = json.loads(data)



    air_flowClient = Airflow.iap_auth()

    run_dag_result = air_flowClient.trigger_dag(
                                        dag_id='demo_dag',
                                        conf=data
                                    )

    return flask.Response(status=202)
    