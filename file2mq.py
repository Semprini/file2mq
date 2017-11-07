#!/usr/bin/env python 
import os, sys, time, json
from queue import Queue
from threading import Thread
import requests
import logging
import traceback

import pika

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

try:
    import settings
except ImportError:
    class settings():
        MQ_FRAMEWORK = {   'USER':'foo',
                        'PASSWORD':'foo',
                        'HOST':'127.0.0.1'
        }
        BASE_QUEUE = 'my_legacy_system_name.from.'
        BASE_PATH = './out/'
        ARCHIVE_PATH = './archive/'

    
class MQHandler():
    def __init__(self):
        self.connect()
        
        
    def connect(self):
        self.credentials = pika.PlainCredentials(settings.MQ_FRAMEWORK['USER'], settings.MQ_FRAMEWORK['PASSWORD'])
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.MQ_FRAMEWORK['HOST'],credentials=self.credentials))
        self.channel = self.connection.channel()
        
        
    def get_headers(self, header_templates, body):
        headers_dict = {}
        #for header in exchange_header_list:
        #    headers_dict[header] = '%s'%getattr(instance,header)
        return headers_dict

        
    def get_exchange_config(self, path):
        filepath, filename = os.path.split(path)
        try:
            with open(filepath + "settings.json") as f:
                config = json.load(f)
        except FileNotFoundError as e:
            exchange_name = settings.BASE_QUEUE + os.path.split(path.replace(settings.BASE_PATH,''))[0]
            config = { 'exchange_name':exchange_name,
                        'header_templates':[]
            }
        return config
        
        
    def send(self, path, body):
        """
        Sends file to exchange
        """
        config = self.get_exchange_config(path)
        headers_dict = self.get_headers(config['header_templates'], body)
        sent = False
        
        while not sent:
            try:
                self.channel.exchange_declare(exchange=config['exchange_name'], exchange_type='headers')
                self.channel.basic_publish(  exchange=config['exchange_name'],
                                    routing_key='',
                                    body=body,
                                    properties = pika.BasicProperties(headers=headers_dict))
                sent = True
            except Exception as err:
                logging.critical( "Unable to send message to exchange: %s | %s"%(err,config['exchange_name']) )
                traceback.print_exc()
                time.sleep(5)
                self.connect()
        
        
def messenger_thread():
    done = False
    mq_handler = MQHandler()
    
    while not done:
        item = q.get()
        if item == "DONE":
            done = True
        else:
            time.sleep(1)
            try:
                stat = os.stat(item)
                with open(item) as f:
                    data = f.read()
                mq_handler.send(item, data)
                
                dest = settings.ARCHIVE_PATH + item.replace(settings.BASE_PATH,'')
                directory = os.path.split(dest)[0]
                if not os.path.exists(directory):
                    os.makedirs(directory)
                os.rename(item, dest)
                logging.info("Sent message %s. Archived to %s", item, dest )
            except FileNotFoundError as e:
                pass
        q.task_done()
    logging.info("Task thread exit")

    
class F2QEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def on_moved(self, event):
        super(F2QEventHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        #logging.info("Moved %s: from %s to %s", what, event.src_path,
        #             event.dest_path)

    def on_created(self, event):
        super(F2QEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        #logging.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event):
        super(F2QEventHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        #logging.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        super(F2QEventHandler, self).on_modified(event)

        try:
            what = 'directory' if event.is_directory else 'file'
            if not event.is_directory and os.path.basename(event.src_path) != 'settings.json':
                stat = os.stat(event.src_path)
                if stat.st_size > 0:
                    q.put(event.src_path)
        except Exception as e:
            logging.error("Unhandled error during modified file handler: {}".format(e) )
        
    
        
if __name__ == "__main__":
    num_worker_threads = 3
    
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    if not os.path.exists(settings.BASE_PATH):
        os.makedirs(settings.BASE_PATH)

    q = Queue()
    for i in range(num_worker_threads):
         t = Thread(target=messenger_thread)
         #t.daemon = True
         t.start()
     
    event_handler = F2QEventHandler()
    observer = Observer()
    observer.schedule(event_handler, settings.BASE_PATH, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for i in range(num_worker_threads):
            q.put("DONE")
        observer.stop()
    observer.join()