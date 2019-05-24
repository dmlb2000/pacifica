from python:3

run pip install cherrypy pika cloudevents uwsgi
copy cherrypy_catch_test.py /usr/src/app/runit.py
expose 8080
cmd uwsgi --http-socket :8080 --wsgi-file /usr/src/app/runit.py
