import unittest
import requests
import socket
import json


class TestSendingDataToES(unittest.TestCase):
    def test_send_metrics(self):
        ES_INDEX = "twitter-streaming-api"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", 11111))
        message = json.dumps({"count_of_tweets_per_min": 5})
        s.sendall(bytes(message, encoding="utf-8"))
        s.shutdown(socket.SHUT_WR)
        received = s.recv(1024)
        received = received.decode("utf-8")
        print("Sent:     {}".format(message))
        print("Received: {}".format(received))
        print("Connection closed.")
        s.close()
        request_to_es = json.dumps({"query": {"match_all": {}},"size": 1,"sort": [{"@timestamp": {"order": "desc"}}]}) # request for getting 1 last element of ES index
        response_from_es = requests.post('http://localhost:9200/twitter-streaming-api/_search',
                                         headers={'Content-Type': 'application/json'},
                                         data=request_to_es)
        count_of_tweets_from_response = response_from_es.json()['hits']['hits'][0]['_source']['count_of_tweets_per_min']
        print("The last count of tweets is " + str(count_of_tweets_from_response))
        self.assertEqual(5,count_of_tweets_from_response)

if __name__ == '__main__':
    unittest.main()