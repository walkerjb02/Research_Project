import requests
from requests.auth import HTTPBasicAuth
from time import sleep
import json

def resetskipindex(): #### NOTE run if this is the first time starting program or you want to reset the pull ####
    with open('Storage.py','w') as file:
        file.write('skip_value = 0\nduplicateprotection = 0')

def auth():
    def token(client_id='', secret=''):
        auth_url = 'https://auth-api.lexisnexis.com/oauth/v2/token'
        payload = ('grant_type=client_credentials&scope=http%3a%2f%2f'
                   'oauth.lexisnexis.com%2fall')
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        json_data = requests.post(auth_url, auth=HTTPBasicAuth(client_id, secret), headers=headers, data=payload).json()
        return json_data['access_token']

    def header(token=token()):
        headers = {'Accept': 'application/json;odata.metadata=minimal',
                   'Connection': 'Keep-Alive',
                   'Host': 'services-api.lexisnexis.com'}
        headers['Authorization'] = 'Bearer ' + token
        return headers
    return header()

def main():
    from Storage import skip_value, duplicateprotection
    authenticate = auth()
    def findID(publication = 'Financial Times'): # <-- Can change publisher here
        url = f"""https://services-api.lexisnexis.com/v1/Sources?$filter=Name eq '{publication}'"""
        r = requests.get(url=url, headers=authenticate)
        return r.json()['value'][0]['Id']
    urlid = findID()
    top = 50 #Max 50
    skip_value, duplicateprotection = int(skip_value), int(duplicateprotection) # Inserts integer at the end of filename as an easy fix for duplicate files
    range = (2014, 2020)
    content = 'News?'
    while 1:
        n, checklst = 0, []
        url = f"""https://services-api.lexisnexis.com/v1/{content}$search=date>={range[0]} and date<={range[1]}&$expand=Document&$filter=SearchType eq LexisNexis.ServicesApi.SearchType'Boolean'  and Source/Id eq '{urlid}'&$top={top}&$skip={skip_value}"""
        r = requests.get(url=url, headers=authenticate)
        print(f'------------------------------------------------------------------------------------------------------------------------------------\n{r}: Total Number Pulled: {skip_value + 50}')
        rjson = r.json()
        while n < top:
            rcontent = rjson['value'][n]
            filename = f"{rcontent['Date'].replace(':', '-')}{duplicateprotection}"
            with open(f"""{filename}.json""", 'w') as file:
                json.dump(rcontent['Document']['Content'],file)
            duplicateprotection += 1
            checklst.append(filename)
            n += 1
        skip_value += top
        with open('Storage.py', 'w') as storage:
            storage.write(f"""skip_value = {skip_value}\nduplicateprotection = {duplicateprotection}""")
        if len(checklst) < 50:
            break
        sleep(12)

if __name__ == "__main__":
    main()
