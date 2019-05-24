#!/usr/bin/python
# -*- coding: utf-8 -*-
"""This is an example CherryPy based event catch used in testing."""
import os
import io
import json
import cherrypy
import pika
from cloudevents.sdk import marshaller
from cloudevents.sdk.event import v01

# pylint: disable=too-few-public-methods
class Root(object):
    """Example root cherrypy class, method dispatcher required."""

    exposed = True

    @cherrypy.tools.json_out()
    # pylint: disable=invalid-name
    # pylint: disable=no-self-use
    def POST(self):
        """Accept the post data and return it."""
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                os.getenv('HOST', 'rabbitmq'),
                int(os.getenv('PORT', '5672')),
                os.getenv('VHOST', '/'),
                pika.PlainCredentials(
                    os.getenv('USER', 'guest'),
                    os.getenv('PASS', 'guest')
                )
            )
        )

        main_channel = connection.channel()
        main_channel.exchange_declare(exchange='gov.pnnl.datahub.events', durable=True, exchange_type='direct')

        headers = cherrypy.request.headers
        headers['Content-Type'] = 'application/cloudevents+json'
        raw_data = cherrypy.request.body.read().decode('utf8')
        print(raw_data)
        data = io.StringIO(raw_data)
        event = v01.Event()
        http_marshaller = marshaller.NewDefaultHTTPMarshaller()
        event = http_marshaller.FromRequest(event, headers, data, lambda x: x)

        print(event.EventType())
        main_channel.basic_publish(
            exchange='gov.pnnl.datahub.events',
            routing_key=event.EventType(),
            body=json.dumps(event.Data()),
            properties=pika.BasicProperties(
                content_type='application/json',
                headers={
                    'cloudEvents:specversion': event.CloudEventVersion(),
                    'cloudEvents:type': event.EventType(),
                    'cloudEvents:id': event.EventID(),
                    'cloudEvents:source': event.Source()
                }
            )
        )
        connection.close()
        return {'message': 'success'}

application = cherrypy.Application(Root(), '/', {
    '/': {
        'request.dispatch': cherrypy.dispatch.MethodDispatcher()
    }
})

if __name__ == '__main__':
    cherrypy.quickstart(Root(), '/', {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher()
        }
    })
