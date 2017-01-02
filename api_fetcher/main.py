# -*- coding: UTF-8 -*-
import sys

if len(sys.argv) != 3:
	print 'ERROR: Wrong number of args.\n'
	usage = 'Usage: python main.py <api_key> <pubsub_topic>\n\n' \
	'Parameters:\n' \
	'\t- api_key: key for Wunderground API\n'\
	'\t- pubsub_topic: topic payload to be pushed to in Pub/Sub\n'
	print usage
	sys.exit(1)

import requests
import json
import os
import time
import logging

from google.cloud import pubsub

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

api_key = sys.argv[1]
pubsub_client = pubsub.Client()
topic_name = sys.argv[2]
pubsub_topic = pubsub_client.topic(topic_name)

if __name__ == '__main__':
	cities = ['Helsinki','Tampere','Oulu','Turku','Jyvaeskylae',
		'Lahti','Kuopio','Kouvola','Pori','Joensuu',
		'Rovaniemi','Utsjoki','Kajaani','Vaasa','Mikkeli',
		'Vaalimaa','Lappeenranta','Kuusamo','Saariselkae','Kittilae']
	base_url = 'http://api.wunderground.com/api/{}/conditions/q/Finland/'
	call_url = base_url.format(api_key)
	req_urls = {city : call_url + city + '.json' for city in cities}

	req_limit_per_min = 10
	counter = 0
	while True:
		logging.info('Starting to batch query data from the Wunderground API. Stop the script with [CTRL+C].')
		topic_payload = ''
		for city, url in req_urls.iteritems():
			logging.debug('Querying data for {}...'.format(city))
			res = json.loads(requests.get(url).content)
			data = json.dumps(res) + '\n'
			topic_payload += data
			counter += 1
			if counter == req_limit_per_min:
				logging.debug('Minute limit reached. Sleeping for 60s.')
				time.sleep(60)
				counter = 0
			time.sleep(2)
		msg_id = pubsub_topic.publish(topic_payload.encode('utf-8'))
		logging.debug('Sent batch to topic {}, message id: {}'.format(topic_name, msd_id))
		logging.debug('Done batch querying data from API. Sleeping for 3600s.')
		time.sleep(3600)
