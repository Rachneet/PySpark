import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import socket

api_key = "4HdnXWnl9zAha1V07jMZTfIdL"
api_secret_key = "MFkUhdnZ01qS4PeoAkFjmi6YLdCdiBhRj7CJbtljRJQ071C4GA"
access_token = "939805826-A01oxgj20jGmDY9Q4HyL8G2miI9K0biPUHbUV7Dp"
access_token_secret = "EotmQRkPW5LNVga1bA8vWd0s2p2a0vmH6XNsCQl1E3jKk"


class TweetListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):

        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("ERROR", e)
        return True

    def on_error(self, status_code):
        print(status_code)
        return True


def sendData(c_socket):
    auth = OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['FPL'])


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 9999
    s.bind((host, port))

    print('listening on port', port)

    s.listen(5)
    c, addr = s.accept()

    sendData(c)
