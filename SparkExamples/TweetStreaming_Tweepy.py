import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key = '' # Use your credential information
consumer_secret = '' # Use your credential information
access_token = '' # Use your credential information
access_secret = '' # Use your credential information

class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])

if __name__ == "__main__":
    s = socket.socket()     # Create a socket object
    host = "localhost"      # Get local machine name
    port = 49742            # Reserve a port for your service.
    s.bind((host, port))    # Bind to the port
    port = s.getsockname()
    print("Listening on port: %s" % str(port))

    s.listen(2)             # Now wait for client connection.
    print("Waiting for a connection")
    c, addr = s.accept()    # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)
