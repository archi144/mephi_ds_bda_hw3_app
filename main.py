from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError
from threading import Timer
import sys
import socket
import json


count_of_tweets = 0

def delete_all_rules(api):
    rule_ids = []
    r = api.request('tweets/search/stream/rules', method_override='GET')
    for item in r:
        if 'id' in item:
            rule_ids.append(item['id'])
        else:
            print(json.dumps(item, indent=2))

    # DELETE STREAM RULES

    if len(rule_ids) > 0:
        r = api.request('tweets/search/stream/rules', {'delete': {'ids': rule_ids}})
        print(f'[{r.status_code}] RULES DELETED: {json.dumps(r.json(), indent=2)}\n')


def send_metrics():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 11111))
    global count_of_tweets
    message = json.dumps({"application"            : "DSDBA_HW_3",
						  "author"                 : "Mechetin Artur",
						  "university"             : "MEPhI",
						  "group"                  : "M19-512",
						  "count_of_tweets_per_min": count_of_tweets})
    s.sendall(bytes(message, encoding="utf-8"))
    s.shutdown(socket.SHUT_WR)
    received = s.recv(1024)
    received = received.decode("utf-8")
    print("Sent:     {}".format(message))
    print("Received: {}".format(received))
    print ("Connection closed.")
    s.close()
    count_of_tweets = 0
    timer = Timer(60, send_metrics)
    timer.start()



def main():
    try:
        if len(sys.argv) > 1:
            query = sys.argv[1]
        else:
            query = 'pizza'
        o = TwitterOAuth.read_file("CREDENTIALS.txt")
        api = TwitterAPI(o.consumer_key, o.consumer_secret, o.access_token_key, o.access_token_secret, auth_type='oAuth2', api_version='2')

        delete_all_rules(api)

        # ADD STREAM RULES

        r = api.request('tweets/search/stream/rules', {'add': [{'value': query}]})
        print(f'[{r.status_code}] RULE ADDED: {r.text}')
        if r.status_code != 201: exit()

        # GET STREAM RULES

        r = api.request('tweets/search/stream/rules', method_override='GET')
        print(f'[{r.status_code}] RULES: {r.text}')
        if r.status_code != 200: exit()

        # START STREAM

        r = api.request('tweets/search/stream')
        print(f'[{r.status_code}] START...')
        if r.status_code != 200: exit()
        global count_of_tweets
        timer = Timer(60, send_metrics)
        timer.start()
        for item in r:
          print(item)
          count_of_tweets += 1


    except TwitterRequestError as e:
        print(e.status_code)
        for msg in iter(e):
            print(msg)

    except TwitterConnectionError as e:
        print(e)

    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()