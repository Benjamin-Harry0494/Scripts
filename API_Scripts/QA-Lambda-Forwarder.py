import json
import os
import http.client

def lambda_handler(event, context):
    if not event.get('headers'):
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"  # Specify JSON content type
            },
            "body": json.dumps({
                "message": "no headers"
            }),
        }
    
    headers = event['headers']

    if not headers.get('x-authorization'):
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"  # Specify JSON content type
            },
            "body": json.dumps({
                "message": "no auth-token provided"
            }),
        }
    
    auth_token = headers['x-authorization']
    print(f"external token : {auth_token}")
    print(f"event : {event}")
    
    if auth_token != os.environ['auth_token']:
        print(f"external token invalid")
        return {
            "headers": {
                "Content-Type": "application/json"  # Specify JSON content type
            },
            "statusCode": 401,
            "body": json.dumps({
                "message": "invalid token"
            }),
        }
        
    if not event.get('body'):  
        print(f"no body for event: {event}")
        return {
            "headers": {
                "Content-Type": "application/json"  # Specify JSON content type
            },
            "statusCode": 200,
            "body": json.dumps({
                "message": "no allocate event"
            }),
        }
        
    body = event['body']
    urls = os.environ['URLS'].split(',')
    body_json = json.loads(body)
    
    results = []
    
    for url in urls:
        try:
            post_headers = set_headers(headers, body_json, url)
            conn = http.client.HTTPSConnection(url + ".qa.patchwork.health")
            path =  "/allocate-inbound"
            print(f"url: {url}, path: {path}, body: {body}, headers: {post_headers}")
            conn.request("POST", path, body=body, headers=post_headers)
            response = conn.getresponse()
            response_data = response.read()
            status_code = response.status
            results.append( {
                "url": url,
                "status_code": status_code,
                "message": response_data
            }   )
            print(response_data.decode("utf-8"))
        except Exception as e:
            print(f"Error: {str(e)}")
            return(f"Error: {str(e)}")
    
    formatted_results = []
    for result in results:
        formatted_result = {
            "url": result['url'],
            "statusCode": result['status_code'],
            "message": result['message'].decode('utf-8') 
        }
        formatted_results.append(formatted_result)
    
    return {
        "statusCode": 202,
        "headers": {
            "Content-Type": "application/json"  # Specify JSON content type
        },
        "body": json.dumps({
            "messages": formatted_results
        })
    }
    
def set_headers(headers, body_json, url):
    post_headers = {} 
    post_headers['x-authorization'] = setXAuth(body_json, url)
    post_headers['content-length'] = headers['content-length']
    post_headers['content-type'] = headers['content-type']
    post_headers['user-agent'] = 'lambda-forwarder'

    return post_headers

def setXAuth(data, url):
    if 'trustCodes' in data and data['trustCodes'] is not None:
        trust_code = data['trustCodes'][0]
        print(f"Using recognised trust codes: {trust_code}")
    else:
        print("no trustcodes found, using default")
        trust_code = 'default'
    
    cases = {
        "RSCH": lambda: setRSCH(url),
        "KCHTRIAL": lambda: setKCH(url),
        "default": lambda: setDefault(url)
    }
    
    auth_token = cases.get(trust_code)()
    print(auth_token)
    return auth_token.strip("'")
    
    
def get_secret(trust_code):
    
    secret_name = f"{trust_code}_Creds"
    region_name = "eu-west-2"
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error: {e}")
        raise e

    return get_secret_value_response['SecretString']

def generate_token(creds, url):
    creds_dict = json.loads(creds)
    graphql_password = creds_dict["Password"]
    graphql_email = creds_dict["Email"]
    graphql_url = f"https://{url}{os.environ['pw_url']}"
    
    graphql_query = """
    mutation hubUserLogin ($email: String!, $password: String!) {
        hubUserLogin (email: $email, password: $password) {
            refreshToken
            token
        }
    }
    """
    
    gql_variables = {
        "email" : graphql_email,
        "password" : graphql_password
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    gql_request = {
        "query": graphql_query,
        "variables": gql_variables
    }

    parsed_url = urlparse(graphql_url)
    hostname = parsed_url.hostname
    
    gql_request_json = json.dumps(gql_request)
    conn = http.client.HTTPSConnection(hostname)
    conn.request("POST", "/graphql", gql_request_json, headers)
    response = conn.getresponse()
    response_data = response.read().decode("utf-8")
    conn.close()
    return response_data
    
    
def setKCH(url):
    trust_code = 'KCH'
    creds = get_secret(trust_code)
    parsed_auth_data = json.loads(creds, url)
    kch_token = parsed_auth_data['data']['hubUserLogin']['token']
    return kch_token

def setRSCH(url):
    trust_code = 'RSCH'
    creds = get_secret(trust_code, url)
    parsed_auth_data = json.loads(creds)
    rsch_token = parsed_auth_data['data']['hubUserLogin']['token']
    return rsch_token
    
def setDefault(url):
    trust_code = 'Default'
    creds = get_secret(trust_code, url)
    parsed_auth_data = json.loads(creds)
    default_token = parsed_auth_data['data']['hubUserLogin']['token']
    return default_token
