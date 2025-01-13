import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('SayHello function processing a request.')
    return func.HttpResponse("Hello from the second function!", status_code=200)