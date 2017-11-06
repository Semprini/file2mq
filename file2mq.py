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
        BASE_PATH = './out/'
        ARCHIVE_PATH = './archive/'

    
class F2q():

    def get_headers(self, header_templates, body):
        headers_dict = {}
        #for header in exchange_header_list:
        #    headers_dict[header] = '%s'%getattr(instance,header)
        return headers_dict

        
    def get_exchange_config(self, path):
        name = os.path.basename(path).split('.')[0]
        config = { 'exchange_name':name,
                    'header_templates':[]
        }
        return config
        
        
    def send(self, path, body):
        """
        Sends file to exchange
        """
        config = self.get_exchange_config(path)
        headers_dict = self.get_headers(config['header_templates'], body)
        
        credentials = pika.PlainCredentials(settings.MQ_FRAMEWORK['USER'], settings.MQ_FRAMEWORK['PASSWORD'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.MQ_FRAMEWORK['HOST'],credentials=credentials))
        channel = connection.channel()
        channel.exchange_declare(exchange=config['exchange_name'], exchange_type='headers')

        channel.basic_publish(  exchange=exchange_name,
                                routing_key='',
                                body=body,
                                properties = pika.BasicProperties(headers=headers_dict))

        connection.close()    
        
        
def messenger_thread():
    done = False
    f2q = F2q()
    
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
                f2q.send(item, data)
                
                dest = settings.ARCHIVE_PATH + item.replace(settings.BASE_PATH,'')
                os.rename(item, dest)
                logging.info("Moved %s: to %s", item, dest )
            except FileNotFoundError as e:
                pass
        logging.info("TASK DONE! {}".format(item))
        q.task_done()
    logging.info("Task thread exit")

    
class F2QEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def on_moved(self, event):
        super(F2QEventHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Moved %s: from %s to %s", what, event.src_path,
                     event.dest_path)

    def on_created(self, event):
        super(F2QEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event):
        super(F2QEventHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        logging.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        super(F2QEventHandler, self).on_modified(event)

        try:
            what = 'directory' if event.is_directory else 'file'
            if not event.is_directory:
                stat = os.stat(event.src_path)
                if stat.st_size > 0:
                    q.put(event.src_path)
        except Exception as e:
            logging.error("Unhandled error during modified file handler: {}".format(e) )
        
    
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    q = Queue()
    t = Thread(target=messenger_thread)
    t.start()

    path = settings.BASE_PATH
    event_handler = F2QEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        q.put("DONE")
        observer.stop()
    observer.join()