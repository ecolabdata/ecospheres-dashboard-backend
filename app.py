def application(environ, start_response):
    data = b"ok"
    status = "200 OK"
    headers = [("Content-type", "text/plain; charset=utf-8"), ("Content-Length", str(len(data)))]
    start_response(status, headers)
    return iter([data])
